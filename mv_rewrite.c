/*-------------------------------------------------------------------------
 *
 * mv_rewrite.c
 *		  Query rewrite for MVs
 *
 * Portions Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "mv_rewrite.h"
#include "equalswalker.h"
#include "extension.h"
#include "join_is_legal.h"

#include "catalog/namespace.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "optimizer/var.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/selfuncs.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "tcop/tcopprot.h"
#include "server/executor/spi.h"
#include "utils/varlena.h"
#include "rewrite/rewriteHandler.h"

PG_MODULE_MAGIC;

#define ListOf(type) List /* type */

typedef struct {
	CustomScanState sstate;
	Value /* String */ *query;
	PlannedStmt *pstmt;
	QueryDesc *qdesc;
	Value /* String */ *competing_cost;
	TupleTableSlot *received;
} mv_rewrite_custom_scan_state;

struct mv_rewrite_dest_receiver
{
	DestReceiver dest;
	mv_rewrite_custom_scan_state *sstate;
};

static bool
mv_rewrite_dest_receiveSlot (TupleTableSlot *slot,
			     DestReceiver *self)
{
	// elog (INFO, "%s DestReceiver (%p)", __func__, self);
    
	struct mv_rewrite_dest_receiver *dest =
    		(struct mv_rewrite_dest_receiver *) self;

	dest->sstate->received = slot;

	return true;
}

static void
mv_rewrite_dest_startup (DestReceiver *self,
			 int operation,
			 TupleDesc typeinfo)
{
}

static void
mv_rewrite_dest_shutdown (DestReceiver *self)
{
}

static void
mv_rewrite_dest_destroy (DestReceiver *self)
{
	// FIXME: should we pfree() ourselves?
}

static void
mv_rewrite_begin_scan (CustomScanState *node,
		       EState *estate,
		       int eflags)
{
	mv_rewrite_custom_scan_state *sstate = (mv_rewrite_custom_scan_state *) node;

	PlannedStmt *pstmt = sstate->pstmt;

	Snapshot    snap;

	if (ActiveSnapshotSet())
		snap = GetActiveSnapshot();
	else
		snap = InvalidSnapshot;
    
	struct mv_rewrite_dest_receiver *dest = palloc (sizeof (struct mv_rewrite_dest_receiver));
	dest->dest.receiveSlot = mv_rewrite_dest_receiveSlot;
	dest->dest.rStartup = mv_rewrite_dest_startup;
	dest->dest.rShutdown = mv_rewrite_dest_shutdown;
	dest->dest.rDestroy = mv_rewrite_dest_destroy;
	dest->sstate = sstate;

	QueryDesc *qdesc = CreateQueryDesc (pstmt,
					    strVal (sstate->query),
					    snap,
					    InvalidSnapshot /* only support query */,
					    (DestReceiver *) dest,
					    NULL, NULL, 0);

	sstate->qdesc = qdesc;

	MemoryContext parent_ctx = CurrentMemoryContext;
	
	ExecutorStart (qdesc, eflags);
	
	MemoryContextSetParent (qdesc->estate->es_query_cxt, parent_ctx);
}

static TupleTableSlot *
mv_rewrite_exec_scan (CustomScanState *node)
{
	mv_rewrite_custom_scan_state *sstate = (mv_rewrite_custom_scan_state *) node;

	QueryDesc *qdesc = sstate->qdesc;

	sstate->received = NULL;

	ExecutorRun (qdesc, ForwardScanDirection, 1, false);
	
	if (sstate->received == NULL)
		return NULL;
	
	TupleTableSlot *slot = node->ss.ps.ps_ResultTupleSlot;
	
	slot = ExecCopySlot (slot, sstate->received);
	
	return slot;
}

static void
mv_rewrite_end_scan (CustomScanState *node)
{
	mv_rewrite_custom_scan_state *sstate = (mv_rewrite_custom_scan_state *) node;

	QueryDesc *qdesc = sstate->qdesc;

	ExecutorFinish (qdesc);
	ExecutorEnd (qdesc);

	FreeQueryDesc (qdesc);

	sstate->qdesc = NULL;
}

static void
mv_rewrite_rescan_scan (CustomScanState *node)
{
	mv_rewrite_custom_scan_state *sstate = (mv_rewrite_custom_scan_state *) node;

	QueryDesc *qdesc = sstate->qdesc;

	ExecutorRewind (qdesc);
}

static void
mv_rewrite_explain_scan (CustomScanState *node,
                                     List *ancestors,
                                     ExplainState *es)
{
	mv_rewrite_custom_scan_state *sstate = (mv_rewrite_custom_scan_state *) node;
    
	// Add rewritten query SQL.
	if (es->verbose)
		ExplainPropertyText("Rewritten", strVal (sstate->query), es);

	// Add competing plan costs.
	if (es->costs)
		ExplainPropertyText("Original costs", strVal (sstate->competing_cost), es);
	
	// Add the subquery scan PS child to allow it to be EXPLAINed.
	// FIXME: we don't do this right now because it results in rather confused output
	// sstate->sstate.custom_ps = list_make1 (sstate->qdesc->planstate);
}

static Bitmapset *
mv_rewrite_all_rels_in_rtable (ListOf (RangeTblEntry) *rtable)
{
	Relids set = NULL;
	
	int i = 0;
	ListCell *lc;
	foreach (lc, rtable)
	{
		set = bms_add_member (set, i);
		i++;
	}
	
	return set;
}

static char *
mv_rewrite_deparse_expression (ListOf (RangeTblEntry *) *rtable,
							   Expr *expr)
{
	return deparse_expression ((Node *) expr,
							   deparse_context_for_plan_rtable (rtable,
																select_rtable_names_for_explain (rtable,
																								 mv_rewrite_all_rels_in_rtable (rtable))),
							   true, false);
}

/*
 * evaluate_query_for_rewrite_support
 *        Check that the query being planned does not involve any features
 *        that we don't know how to process.
 */
static bool
mv_rewrite_eval_query_for_rewrite_support (PlannerInfo *root)
{
	if (root->hasRecursion)
		return false;

	if (root->hasLateralRTEs)
		return false;
	
	if (root->distinct_pathkeys != NULL &&
		list_length (root->distinct_pathkeys) > 0)
		return false;

	if (root->cte_plan_ids != NULL &&
		list_length (root->cte_plan_ids) > 0)
		return false;

	// Default is that we _do_ support rewrite.
	return true;
}

static void
mv_rewrite_add_rewritten_mv_paths (PlannerInfo *root,
                             RelOptInfo *input_rel, // if grouping
                             RelOptInfo *inner_rel, RelOptInfo *outer_rel, // if joining
                             RelOptInfo *grouped_rel,
                             PathTarget *grouping_target);

/*
 * mv_rewrite_create_upper_paths_hook
 *        Add paths for post-join operations like aggregation, grouping etc. if
 *        corresponding operations are safe to push down.
 *
 * Right now, we only support aggregate, grouping and having clause pushdown.
 */
void
mv_rewrite_create_upper_paths_hook(PlannerInfo *root,
			      UpperRelationKind stage,
			      RelOptInfo *input_rel,
			      RelOptInfo *output_rel)
{
	// Delegate first to any other extensions if they are already hooked.
	if (next_create_upper_paths_hook)
		(*next_create_upper_paths_hook) (root, stage, input_rel, output_rel);

	// Ignore stages we don't support; and skip any duplicate calls.
	if (stage != UPPERREL_GROUP_AGG)
	{
		if (g_trace_match_progress)
			elog(INFO, "%s: upper path stage (%d) not supported.", __func__, stage);
		
		return;
	}
	
	// Evaluate the query being planned for any unsupported features.
	if (!mv_rewrite_eval_query_for_rewrite_support (root))
	{
		if (g_log_match_progress)
			elog(INFO, "%s: query requires unsupported features.", __func__);
		
		return;
	}
	
	if (input_rel->cheapest_total_path != NULL)
		if (g_rewrite_minimum_cost > 0.0)
			if (input_rel->cheapest_total_path->total_cost < g_rewrite_minimum_cost)
			{
				if (g_log_match_progress)
					elog(INFO, "%s: already have path with acceptable cost.", __func__);

				return;
			}
	
	if (g_trace_match_progress)
	{
		elog(INFO, "%s: stage: %d", __func__, stage);
		elog(INFO, "%s: root: %s", __func__, nodeToString (root));
		elog(INFO, "%s: input_rel: %s", __func__, nodeToString (input_rel));
		elog(INFO, "%s: output_rel: %s", __func__, nodeToString (output_rel));
	}

	// If we can rewrite, add those alternate paths...
	PathTarget *grouping_target = root->upper_targets[UPPERREL_GROUP_AGG];

	mv_rewrite_add_rewritten_mv_paths(root, input_rel, NULL, NULL, output_rel, grouping_target);
}

static Bitmapset *
recurse_relation_relids_from_rte (PlannerInfo *root, RangeTblEntry *rte, Bitmapset *out_relids)
{
	if (rte->rtekind == RTE_RELATION)
	{
		//elog(INFO, "%s: add relation: %d", __func__, rte->relid);
		out_relids = bms_add_member (out_relids, (int) rte->relid);
	}
	else if (rte->rtekind == RTE_SUBQUERY)
	{
		if (rte->subquery != NULL)
			if (rte->subquery->rtable != NULL)
			{
				ListCell *lc;
				foreach (lc, rte->subquery->rtable)
				{
					RangeTblEntry *srte = (RangeTblEntry *) lfirst (lc);
					
					// FIXME: should not use BMS to store the large integers that are Oids!
					out_relids = recurse_relation_relids_from_rte (root, srte, out_relids);
				}
			}
	}
	return out_relids;
}

static Bitmapset *
recurse_relation_oids_from_rte (PlannerInfo *root, RangeTblEntry *rte, Bitmapset *relids);

static Bitmapset *
recurse_relation_oids (PlannerInfo *root, Bitmapset *initial_relids, Bitmapset *out_relids)
{
    for (int x = bms_next_member (initial_relids, -1); x >= 0; x = bms_next_member (initial_relids, x))
    {
        RangeTblEntry *rte = planner_rt_fetch(x, root);
        //elog(INFO, "%s: recurse relid %d (rtekind %d)", __func__, x, rte->rtekind);
        out_relids = recurse_relation_oids_from_rte (root, rte, out_relids);
    }
    
    return out_relids;
}

static Bitmapset *
recurse_relation_oids_from_rte (PlannerInfo *root, RangeTblEntry *rte, Bitmapset *out_relids)
{
    if (rte->rtekind == RTE_RELATION)
    {
        //elog(INFO, "%s: add relation: %d", __func__, rte->relid);
        out_relids = bms_add_member (out_relids, (int) rte->relid);
    }
    else if (rte->rtekind == RTE_SUBQUERY)
    {
        if (rte->subquery != NULL)
            if (rte->subquery->rtable != NULL)
            {
                ListCell *lc;
                foreach (lc, rte->subquery->rtable)
                {
                    RangeTblEntry *srte = (RangeTblEntry *) lfirst (lc);
					
					// FIXME: should not use BMS to store the large integers that are Oids!
                    out_relids = recurse_relation_relids_from_rte (root, srte, out_relids);
                }
            }
    }
    return out_relids;
}

static ListOf (char *) *
mv_rewrite_get_involved_rel_names (PlannerInfo *root,
								   RelOptInfo *inner_rel,
								   RelOptInfo *input_rel, RelOptInfo *outer_rel)
{
	Bitmapset *rel_oids = NULL;

	if (input_rel != NULL)
		rel_oids = recurse_relation_oids (root, input_rel->relids, NULL);
	else if (inner_rel != NULL && outer_rel != NULL)
	{
		rel_oids = recurse_relation_oids (root, inner_rel->relids, NULL);
		rel_oids = recurse_relation_oids (root, outer_rel->relids, rel_oids);
	}
	
	//elog(INFO, "%s: relids=%s", __func__, bmsToString (relids));
	
	ListOf (char *) *tables_strlist = NIL;
	for (int x = bms_next_member (rel_oids, -1); x >= 0; x = bms_next_member (rel_oids, x))
	{
		StringInfo table_name = makeStringInfo();
		
		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		Relation	rel = heap_open((Oid) x, NoLock);
		
		heap_close(rel, NoLock);
		
		const char *nspname = get_namespace_name (RelationGetNamespace(rel));
		const char *relname = RelationGetRelationName(rel);
		
		appendStringInfo (table_name, "%s.%s",
						  quote_identifier(nspname), quote_identifier(relname));
		
		tables_strlist = lappend (tables_strlist, table_name->data);
	}
	return tables_strlist;
}

static void
mv_rewrite_find_related_mvs_for_rel (ListOf (char *) *involved_rel_names,
                                     ListOf (StringInfo) **mvs_schema,
                                     ListOf (StringInfo) **mvs_name)
{
    *mvs_schema = NIL, *mvs_name = NIL;
    
	StringInfoData find_mvs_query;
	initStringInfo (&find_mvs_query);
	
	appendStringInfoString (&find_mvs_query,
							"SELECT v.schemaname, v.matviewname"
							" FROM "
							"   public.pgx_rewritable_matviews j, pg_matviews v"
							" WHERE "
							" j.matviewschemaname = v.schemaname"
							" AND j.matviewname = v.matviewname"
							" AND j.tables @> $1");
	
    ArrayType /* text */ *tables_arr = strlist_to_textarray (involved_rel_names);
    
    Oid argtypes[] = { TEXTARRAYOID };
    Datum args[] = { PointerGetDatum (tables_arr) };

    // We must be careful to manage the SPI memory context.
    
    MemoryContext mainCtx = CurrentMemoryContext;
    
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(WARNING, "%s: SPI_connect() failed", __func__);
        return; // (an empty set of matviews)
    }
    
    SPIPlanPtr plan = SPI_prepare (find_mvs_query.data, 1, argtypes);
    
    if (plan == NULL) {
        elog(WARNING, "%s: SPI_connect() failed", __func__);
        return; // (an empty set of matviews)
    }
    
    Portal port = SPI_cursor_open (NULL, plan, args, NULL, true);
    
    for (SPI_cursor_fetch (port, true, 1);
         SPI_processed >= 1;
         SPI_cursor_fetch (port, true, 1))
    {
        MemoryContext spiCtx = MemoryContextSwitchTo (mainCtx);
        
        StringInfo schema = makeStringInfo();
        appendStringInfoString (schema, SPI_getvalue (SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
        StringInfo name = makeStringInfo();
        appendStringInfoString (name, SPI_getvalue (SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));

        *mvs_schema = lappend (*mvs_schema, schema);
        *mvs_name = lappend (*mvs_name, name);
        
        MemoryContextSwitchTo (spiCtx);
    }

    SPI_cursor_close (port);

    SPI_finish();
}

static Query *
mv_rewrite_rewrite_mv_query (Query *query)
{
	if (g_trace_parse_select_query)
		elog(INFO, "%s: parse tree: %s", __func__, nodeToString (query));
	
	/* Lock and rewrite, using a copy to preserve the original query. */
	Query *copied_query = (Query *) copyObject (query);
	AcquireRewriteLocks (copied_query, true, false);
	ListOf (Query *) *rewritten = QueryRewrite (copied_query);
	
	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length (rewritten) != 1)
		elog(ERROR, "unexpected rewrite result");
	query = (Query *) linitial (rewritten);

	if (false) {
	ListCell *e;
	foreach (e, query->targetList)
	{
		TargetEntry *tle = lfirst_node (TargetEntry, e);
		
		if (g_trace_parse_select_query)
			elog(INFO, "%s: simplifying: %s", __func__, nodeToString (tle));
		
		Expr *new_expr = (Expr *) eval_const_expressions (NULL, (Node *) tle->expr);
		
		tle->expr = new_expr;
		
		if (g_trace_parse_select_query)
			elog(INFO, "%s: revised: %s", __func__, nodeToString (tle));
	}
	}
	
	if (g_trace_parse_select_query)
		elog(INFO, "%s: query: %s", __func__, nodeToString (query));
	
	return query;
}

static void
mv_rewrite_get_mv_query (RelOptInfo *grouped_rel,
			  const char *mv_name,
			  Query **parsed_mv_query,
			  Oid *matviewOid)
{
    RangeVar    *matviewRV;
    Relation    matviewRel;
    RewriteRule *rule;
    List       *actions;
    Query       *query;

    LOCKMODE    lockmode = NoLock;

    matviewRV = makeRangeVarFromNameList (stringToQualifiedNameList (mv_name));

    *matviewOid = RangeVarGetRelid (matviewRV, lockmode, false);

    matviewRel = heap_open (*matviewOid, lockmode);
    
    /* Make sure it is a materialized view. */
    if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW)
        elog(ERROR, "\"%s\" is not a materialized view", RelationGetRelationName(matviewRel));

    /* We don't allow an oid column for a materialized view. */
    Assert(!matviewRel->rd_rel->relhasoids);
    
    /*
     * Check that everything is correct for a refresh. Problems at this point
     * are internal errors, so elog is sufficient.
     */
    if (matviewRel->rd_rel->relhasrules == false ||
        matviewRel->rd_rules->numLocks < 1)
        elog(ERROR, "materialized view \"%s\" is missing rewrite information", RelationGetRelationName(matviewRel));
    
    if (matviewRel->rd_rules->numLocks > 1)
        elog(ERROR, "materialized view \"%s\" has too many rules", RelationGetRelationName(matviewRel));
    
    rule = matviewRel->rd_rules->rules[0];
    if (rule->event != CMD_SELECT || !(rule->isInstead))
        elog(ERROR, "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule", RelationGetRelationName(matviewRel));
    
    actions = rule->actions;
    if (list_length(actions) != 1)
        elog(ERROR, "the rule for materialized view \"%s\" is not a single action", RelationGetRelationName(matviewRel));
    
    query = linitial_node (Query, actions);
    
    heap_close (matviewRel, lockmode);

    if (g_trace_parse_select_query)
        elog(INFO, "%s: parse tree: %s", __func__, nodeToString (query));
	
	query = mv_rewrite_rewrite_mv_query (query);

    *parsed_mv_query = query;
}

struct mv_rewrite_xform_todo_list {
    ListOf (Index) *replacement_var;
    ListOf (Expr *) *replaced_expr;
	Index replacement_varno;
};

static void
mv_rewrite_add_xform_todo_list_entry (struct mv_rewrite_xform_todo_list *todo_list, Index i, Expr *replaced_node, TargetEntry *replacement_te)
{
    //elog(INFO, "%s: will replace node %s: with Var (varattrno=%d)", __func__, nodeToString (replaced_node), i);
	if (todo_list == NULL)
		return;

	todo_list->replacement_var = lappend_int(todo_list->replacement_var, (int) i);
    todo_list->replaced_expr = lappend(todo_list->replaced_expr, replaced_node);
}

static Node *
mv_rewrite_xform_todos_mutator (Node *node, struct mv_rewrite_xform_todo_list *todo_list)
{
    ListCell *lc1, *lc2;
    forboth(lc1, todo_list->replaced_expr, lc2, todo_list->replacement_var)
    {
        Expr *replaced_node = (Expr *) lfirst (lc1);
        Index i = (Index) lfirst_int (lc2);
        
        if (replaced_node == (Expr *) node)
        {
            
            Var *v = makeNode (Var);
			
			// The Var in the replacement node will always reference Level 1
			// because we replace the query/Rel with a simple SELECT subquery
			// that pulls data from the MV.
            v->varno = v->varnoold = todo_list->replacement_varno;
			
            v->varattno = v->varoattno = (int) i;
            v->varcollid = exprCollation ((Node *) replaced_node);
            v->varlevelsup = 0;
			v->vartype = exprType ((Node *) replaced_node);
            v->vartypmod = -1;
            v->location = -1;
            
            //elog(INFO, "%s: transforming node: %s into: %s", __func__, nodeToString (replaced_node), nodeToString (v));
            
            return (Node *) v;
        }
    }
    return expression_tree_mutator (node, mv_rewrite_xform_todos_mutator, todo_list);
}

static ListOf (TargetEntry *) *
mv_rewrite_xform_todos (ListOf (TargetEntry *) *tlist,
						struct mv_rewrite_xform_todo_list *todo_list)
{
    return (List *) expression_tree_mutator ((Node *) tlist, mv_rewrite_xform_todos_mutator, todo_list);
}

static bool
mv_rewrite_check_expr_targets_in_mv_tlist (PlannerInfo *root,
                                     Query *parsed_mv_query,
                                     Node *expr,
                                     struct mv_rewrite_xform_todo_list *todo_list,
                                     bool trace);

struct mv_rewrite_expr_targets_equals_ctx
{
    PlannerInfo *root;
    List *parsed_mv_query_rtable; // parsed_mv_query->rtable;
};

static bool
mv_rewrite_expr_targets_equals_walker (Node *a, Node *b, struct mv_rewrite_expr_targets_equals_ctx *ctx)
{
    //elog (INFO, "%s: comparing: %s with: %s", __func__, nodeToString (a), nodeToString (b));
    
    if (a != NULL && b != NULL && nodeTag(a) == nodeTag(b) && nodeTag(a) == T_Var)
    {
        //elog (INFO, "%s: comparing Var", __func__);

		// FIXME: and also compare levelsup, varno's oid
        
        Value *a_col_name = list_nth_node(Value,
                                          planner_rt_fetch((int) ((Var *)a)->varno, ctx->root)->eref->colnames,
                                          (int) ((Var *)a)->varattno - 1);
        
        Value *b_col_name = list_nth_node(Value,
                                          list_nth_node (RangeTblEntry, ctx->parsed_mv_query_rtable, (int) ((Var *)b)->varno - 1)->eref->colnames,
                                          (int) ((Var *)b)->varattno - 1);
        
        if (0 == strcmp (a_col_name->val.str, b_col_name->val.str))
        {
            //elog(INFO, "%s: match for Var: %d/%d->%d/%d (%s)", __func__, ((Var *)a)->varno, ((Var *)a)->varattno, ((Var *)b)->varno, ((Var *)b)->varattno, a_col_name->val.str);
            return true;
        }
        else
        {
            //elog(INFO, "%s: no match for Var: %d/%d, %d/%d (%s, %s)", __func__, ((Var *)a)->varno, ((Var *)a)->varattno, ((Var *)b)->varno, ((Var *)b)->varattno, a_col_name->val.str, b_col_name->val.str);
            return false;
        }
    }
    
    bool result = equal_tree_walker(a, b, mv_rewrite_expr_targets_equals_walker, ctx);
    
    if (result)
    {
        //elog(INFO, "%s: matched", __func__);
    }
    else
    {
        //elog(INFO, "%s: not matched", __func__);
    }
    
    return result;
}

struct mv_rewrite_expr_targets_in_mv_tlist_ctx
{
    ListOf (TargetEntry *) *tList;
    bool trace; // enable trace logging
    bool match_found;
    bool did_search_anything;
    struct mv_rewrite_xform_todo_list *todo_list;
    struct mv_rewrite_expr_targets_equals_ctx *col_names;
};

static bool
mv_rewrite_expr_targets_in_mv_tlist_walker (Node *node, struct mv_rewrite_expr_targets_in_mv_tlist_ctx *ctx)
{
    bool done = false;
    bool local_match_found = false;
    
    if (ctx->trace)
        elog(INFO, "%s: >>>>", __func__);
    
    ctx->did_search_anything = true;
    
    if (!ctx->match_found)
    {
        if (ctx->trace)
            elog(INFO, "%s: previous expr didn't match; no point continuing...", __func__);

        return false;
    }
    
    if (ctx->trace)
        elog(INFO, "%s: checking expr: %s", __func__, nodeToString (node));
    
    if (!done)
    {
        if (node == NULL)
        {
            done = true;
            
            if (ctx->trace)
                elog(INFO, "%s: NULL node", __func__);
        }
    }
    
    if (!done)
    {
        if (IsA (node, Const))
        {
            local_match_found = true;
            done = true;
            
            if (ctx->trace)
                elog(INFO, "%s: constants are always acceptable", __func__);
        }
    }
    
    if (!done)
    {
        // First check if any of the expressions match any TL node outright.
        // It might, for example, be a Var, and that should match easily.
        Index i = 1;
        ListCell *lc;
        foreach (lc, ctx->tList)
        {
            TargetEntry *te = lfirst(lc);
            
            if (ctx->trace)
                elog(INFO, "%s: against expr: %s", __func__, nodeToString (te->expr));
            
            local_match_found = mv_rewrite_expr_targets_equals_walker ((Node *) node, (Node *) te->expr, ctx->col_names);
            
            if (local_match_found)
            {
                if (ctx->trace)
                    elog(INFO, "%s: matched!", __func__);
				
                mv_rewrite_add_xform_todo_list_entry (ctx->todo_list, i, (Expr *) node, te);
                
                done = true;
                
                break;
            }
            i++;
        }
    }
    
    if (!done)
    {
        // Copy current context
        struct mv_rewrite_expr_targets_in_mv_tlist_ctx sub_ctx = *ctx;
        
        sub_ctx.match_found = true; // walker will set to false if not matched
        sub_ctx.did_search_anything = false; // indicates whether match_found is valid
        
        if (ctx->trace)
            elog(INFO, "%s: walking sub-expressions...", __func__);
        
        // Look at the sub-expressions inside the Node. All must either match
        // exactly, or derive from something that does.
        expression_tree_walker (node, mv_rewrite_expr_targets_in_mv_tlist_walker, &sub_ctx);
        
        if (!sub_ctx.did_search_anything) {
            if (ctx->trace)
                elog(INFO, "%s: (no sub-expressions walked)", __func__);
        }
        
        if (ctx->trace)
            elog(INFO, "%s: done walking sub-expressions.", __func__);
        
        local_match_found = sub_ctx.did_search_anything && sub_ctx.match_found;
        
        done = true;
    }
    
    if (done)
    {
        if (ctx->trace)
            elog(INFO, "%s: done; local match found: %d", __func__, local_match_found);
        
        // We got a match: record it, but we can't abort the walk because
        // there may be adjacent arguments. Unfortunately, we can't directly
        // signal to go no deepper, but continue with the adjacent walk.
        ctx->match_found = ctx->match_found && local_match_found;
        
        if (ctx->trace)
            elog(INFO, "%s: done; resulting match indicator: %d", __func__, ctx->match_found);
    }
    
    if (ctx->trace)
        elog(INFO, "%s: <<<<", __func__);
    
    // Always continue to search adjacent expressions, even if we didn't dive down.
    return false;
}

static bool
mv_rewrite_check_expr_targets_in_mv_tlist (PlannerInfo *root,
                                     Query *parsed_mv_query,
                                     Node *expr,
                                     struct mv_rewrite_xform_todo_list *todo_list,
                                     bool trace)
{
    if (expr == NULL)
        return true;
    
    struct mv_rewrite_expr_targets_equals_ctx col_names = {
        root, parsed_mv_query->rtable
    };
    struct mv_rewrite_expr_targets_in_mv_tlist_ctx ctx = {
        parsed_mv_query->targetList, trace
    };
    ctx.match_found = true; // walker will set to false if not matched
    ctx.todo_list = todo_list;
    ctx.col_names = &col_names;
    
    mv_rewrite_expr_targets_in_mv_tlist_walker ((Node *) expr, &ctx);
    
    return ctx.match_found;
}

static ListOf (Value *) *
mv_rewrite_get_query_tlist_colnames (Query *mv_query)
{
    ListOf (Value *) *colnames = NIL;
    
    ListCell *mv_lc;
    foreach (mv_lc, mv_query->targetList)
    {
        TargetEntry *mv_tle = lfirst_node (TargetEntry, mv_lc);
        
        colnames = lappend (colnames, makeString (mv_tle->resname));
    }
    
    return colnames;
}

static bool
mv_rewrite_check_group_clauses_for_mv (PlannerInfo *root,
                                 Query *parsed_mv_query,
                                 RelOptInfo *grouped_rel,
                                 List *transformed_tlist,
                                 struct mv_rewrite_xform_todo_list *todo_list)
{
    //elog(INFO, "%s: target list: %s", __func__, nodeToString(transformed_tlist));
    
    // Check each GROUP BY clause (Expr) in turn...
    ListCell   *lc;
    foreach (lc, root->parse->groupClause)
    {
        SortGroupClause *sgc = lfirst (lc);
        Expr *expr = list_nth_node(TargetEntry, transformed_tlist, (int) sgc->tleSortGroupRef - 1)->expr;
        
        //elog(INFO, "%s: checking index %d node: %s", __func__, sgc->tleSortGroupRef, nodeToString(expr));
        
        // Check that the Expr either direclty matches an TLE Expr in the MV target list, or
        // is an expression that builds upon one of them.
        //
        // Returns a to do list of Expr nodes that must be rewritten as a Var to reference the
        // MV TLE directly.
        if (!mv_rewrite_check_expr_targets_in_mv_tlist (root, parsed_mv_query, (Node *) expr, todo_list, g_trace_group_clause_source_check))
        {
            if (g_log_match_progress)
                elog(INFO, "%s: GROUP BY clause (%s) not found in MV SELECT list", __func__,
					 mv_rewrite_deparse_expression (root->parse->rtable, expr));
			
            return false;
        }
    }

    return true;
}

static bool
mv_rewrite_join_node_is_valid_for_plan_recurse (PlannerInfo *root,
								  Node *node,
			  					  Query *parsed_mv_query,
								  Relids join_relids,
								  List **mv_oids_involved,
								  Relids *query_relids_involved,
								  RelOptInfo **query_found_rel,
								  ListOf (RestrictInfo *) **query_clauses,
								  ListOf (RestrictInfo *) **collated_mv_quals);

/**
 * Check for the presence of every join clause from the MV in the query plan.
 * If a clause is missing, then it means the MV cannot substitute for the query.
 * All 'found' clauses are removed from the query_clauses list.
 *
 * It is equally possible the query plan contained more clauses than the MV. Those
 * additional clauses are left in query_clauses.
 */
static bool
mv_rewrite_join_clauses_are_valid (struct mv_rewrite_expr_targets_equals_ctx *comparison_context,
					ListOf (RestrictInfo * or Expr *) *mv_join_quals,
					ListOf (RestrictInfo * or Expr *) **query_clauses)
{
	ListOf (RestrictInfo * or Expr *) *query_clauses_copy = list_copy (*query_clauses);
	
	if (g_debug_join_clause_check)
	{
		StringInfo cl = makeStringInfo();
		ListCell *lc;
		foreach (lc, query_clauses_copy)
		{
			RestrictInfo *query_ri = lfirst (lc);
			Expr *query_expr = (Expr *) query_ri;
			
			/* Extract clause from RestrictInfo, if required */
			if (IsA (query_ri, RestrictInfo))
				query_expr = query_ri->clause;
			if (cl->len > 0)
				appendStringInfoString (cl, ", ");
			appendStringInfoString (cl, mv_rewrite_deparse_expression (comparison_context->root->parse->rtable, query_expr));
		}
		elog (INFO, "%s: clause list to check: %s", __func__, cl->data);
	}

	ListCell *lc2;
	foreach (lc2, mv_join_quals)
	{
		Expr *mv_jq = lfirst (lc2);
		
		if (IsA (mv_jq, RestrictInfo))
			mv_jq = ((RestrictInfo *) mv_jq)->clause;
		
		if (IsA (mv_jq, BoolExpr) &&
			((BoolExpr *) mv_jq)->boolop == AND_EXPR)
		{
			mv_join_quals = list_concat_unique_ptr (mv_join_quals, ((BoolExpr *) mv_jq)->args);
			continue;
		}
		
		if (g_debug_join_clause_check)
			elog(INFO, "%s: evaluating JOIN expression: %s", __func__, nodeToString (mv_jq));
		
		bool found = false;
		
		ListCell *lc3;
		foreach (lc3, query_clauses_copy)
		{
			RestrictInfo *query_ri = lfirst (lc3);
			Expr *query_expr = (Expr *) query_ri;
			
			/* Extract clause from RestrictInfo, if required */
			if (IsA (query_ri, RestrictInfo))
				query_expr = query_ri->clause;
			
			if (g_trace_join_clause_check)
				elog(INFO, "%s: against expression: %s", __func__,
					 mv_rewrite_deparse_expression (comparison_context->root->parse->rtable, query_expr));
			
			if (mv_rewrite_expr_targets_equals_walker ((Node *) query_expr, (Node *) mv_jq, comparison_context))
			{
				if (g_debug_join_clause_check)
					elog(INFO, "%s: matched expression: %s", __func__, nodeToString (query_expr));
				
				*query_clauses = list_delete_ptr (*query_clauses, query_ri);
				
				found = true;
				break;
			}
		}
		
		if (!found)
		{
			if (g_log_match_progress)
				elog(INFO, "%s: MV expression (%s) not found in clauses in the query.", __func__,
					 mv_rewrite_deparse_expression (comparison_context->parsed_mv_query_rtable, mv_jq));
			
			return false;
		}
	}
	
	return true;
}

static bool
mv_rewrite_join_node_is_valid_for_plan (PlannerInfo *root,
						  Query *parsed_mv_query,
					      RelOptInfo *join_rel,
						  ListOf (RestrictInfo * or Expr *) **additional_where_clauses)
{
	if (g_debug_join_clause_check)
		elog(INFO, "%s: checking join MV's join tree is valid for the query plan...", __func__);

	if (g_trace_join_clause_check)
		elog(INFO, "%s: join_rel: %s", __func__, nodeToString (join_rel));

	// Attempt to find a legal join in the plan for each of the joins in the MV's jointree.
	
	ListOf (Oid) *mv_oids_involved = NIL;
	Relids query_relids_involved = NULL;
	
	Relids join_relids = join_rel->relids;
	
	if (g_trace_join_clause_check)
		elog(INFO, "%s: join_relids: %s", __func__, bmsToString (join_relids));
	
	// As we walk the join tree, build up a list of quals. We will check each qual
	// for presence in the query later.
	ListOf (RestrictInfo *) *collated_query_quals = NIL;
	ListOf (RestrictInfo *) *collated_mv_quals = NIL;

	if (!mv_rewrite_join_node_is_valid_for_plan_recurse (root, (Node *) parsed_mv_query->jointree, parsed_mv_query, join_relids, &mv_oids_involved, &query_relids_involved, NULL, &collated_query_quals, &collated_mv_quals))
		return false;

	struct mv_rewrite_expr_targets_equals_ctx comparison_context = {
		root, parsed_mv_query->rtable
	};
	
	// Check each MV join clauses is present in the query plan.
	if (!mv_rewrite_join_clauses_are_valid (&comparison_context, collated_mv_quals, &collated_query_quals))
		return false;
	
	// Any query plan clauses not found will have to be folded as quals against the rewritten query.
	*additional_where_clauses = collated_query_quals;
	
	// The join should now have accounted for every rel in the join_rel: if not, we don't have a match
	if (!bms_equal (join_relids, query_relids_involved))
	{
		if (g_log_match_progress)
			elog(INFO, "%s: the query does not involve all relations joined by the MV", __func__);

		return false;
	}
	
	return true;
}

static bool
mv_rewrite_rte_equals_walker (Node *a, Node *b, void *ctx)
{
	if (a != NULL && b != NULL && nodeTag (a) == nodeTag (b) && nodeTag (a) == T_RangeTblEntry)
	{
		// Shallow copy before we fixup...
		RangeTblEntry my_a = *castNode (RangeTblEntry, a);
		RangeTblEntry my_b = *castNode (RangeTblEntry, b);
		
		// Best case is they match already. But anticipate a mismatching checkAsUser...
		my_a.checkAsUser = my_b.checkAsUser = (Oid) 0;
		// ...same for selectedCols...
		my_a.selectedCols = my_b.selectedCols = NULL;

		return equal_tree_walker (&my_a, &my_b, mv_rewrite_rte_equals_walker, ctx);
	}
	else if (a != NULL && b != NULL && nodeTag (a) == nodeTag (b) && nodeTag (a) == T_Alias)
	{
		Alias my_a = *castNode (Alias, a);
		Alias my_b = *castNode (Alias, b);

		// ...and alias name.
		my_a.aliasname = my_b.aliasname = "dummy";
		
		return equal_tree_walker (&my_a, &my_b, mv_rewrite_rte_equals_walker, ctx);
	}
	
	return equal_tree_walker(a, b, mv_rewrite_rte_equals_walker, ctx);
}

ListOf (RestrictInfo *) *
mv_rewrite_get_pushed_down_exprs (PlannerInfo *root,
								  RelOptInfo *rel,
								  Index relid)
{
	ListOf (Expr *) *out_ris = NIL;

	ListOf (RestrictInfo *) *rel_ris = NIL;

	if (IS_SIMPLE_REL (rel))
		rel_ris = rel->baserestrictinfo;
	else if (IS_JOIN_REL (rel))
		rel_ris = rel->joininfo;
	
	ListCell *lc;
	foreach (lc, rel_ris)
	{
		RestrictInfo *rel_ri = lfirst_node (RestrictInfo, lc);
		
		if (rel_ri->is_pushed_down)
		{
			if (g_trace_pushed_down_clauses_collation)
				elog (INFO, "%s: found pushed down clause: %s", __func__, mv_rewrite_deparse_expression (root->parse->rtable, rel_ri->clause));

			// The RI Vars are live structures in the query plan, so take a copy before
			// perfroming any transformation on them.
			out_ris = lappend (out_ris, copyObject (rel_ri->clause));
		}
	}
	
	// FIXME: not sure if we should specially handle any other rtekinds.
	
	if (rel->rtekind != RTE_SUBQUERY)
		goto done;
	
	PlannerInfo *subroot = rel->subroot;

	ListOf (Expr *) *sub_ris = NIL;
	
	for (int i = 1; i < subroot->simple_rel_array_size; i++)
	{
		RelOptInfo *subrel = subroot->simple_rel_array[i];
		
		if (subrel != NULL && IS_SIMPLE_REL (subrel))
		{
			ListOf (RestrictInfo *) *subris = mv_rewrite_get_pushed_down_exprs (rel->subroot, subrel, (Index) i);

			sub_ris = list_concat (sub_ris, subris);
		}
	}
	
	if (g_trace_pushed_down_clauses_collation)
	{
		StringInfo cl = makeStringInfo();
		ListCell *lc;
		foreach (lc, sub_ris)
		{
			if (cl->len > 0)
				appendStringInfoString (cl, ", ");
			appendStringInfoString (cl, mv_rewrite_deparse_expression (subroot->parse->rtable, (Expr *) lfirst (lc)));
		}
		elog (INFO, "%s: collated clause list: %s", __func__, cl->data);
	}

	// Done with this query level: thin out the list, and include only those
	// RI clauses that can be pulled back up. We do this by matching each
	// calsue against the returning subquery's target list.
	
	struct mv_rewrite_xform_todo_list xform_todo_list = {
		.replaced_expr = NIL,
		.replacement_var = NIL,
		.replacement_varno = relid
	};
	
	ListCell *lc2;
	foreach (lc2, sub_ris)
	{
		Expr *rel_ri_expr = (Expr *) lfirst (lc2);
		
		// FIXME: we should check that none of the Expr's Vars references a levelsup > 0
		
		if (!mv_rewrite_check_expr_targets_in_mv_tlist (subroot, subroot->parse, (Node *) rel_ri_expr, &xform_todo_list, false))
		{
			if (g_trace_pushed_down_clauses_collation)
				elog (INFO, "%s: discounting pushed down clause due to no match in above subquery target list: %s", __func__,
					  mv_rewrite_deparse_expression (subroot->parse->rtable, rel_ri_expr));
			
			sub_ris = list_delete (sub_ris, rel_ri_expr);
		}
	}
	
	// Rewrite RIs from the subquery, such that their Vars reference the subquery's target list,
	// which has the effect of pulling them up a level.
	sub_ris = mv_rewrite_xform_todos (sub_ris, &xform_todo_list);

	out_ris = list_concat (out_ris, sub_ris);
	
	if (g_trace_pushed_down_clauses_collation)
	{
		StringInfo cl = makeStringInfo();
		ListCell *lc;
		foreach (lc, out_ris)
		{
			if (cl->len > 0)
				appendStringInfoString (cl, ", ");
			appendStringInfoString (cl, mv_rewrite_deparse_expression (root->parse->rtable, (Expr *) lfirst (lc)));
		}
		elog (INFO, "%s: resulting clause list: %s", __func__, cl->data);
	}

done:
	return out_ris;
}

/**
 * Recursively descend into the MV's jointree, and do two things:
 *
 * 1. For each join, ensure there is a matching rel in the planned query.
 * 2. Aggregate the quals from both the MV join tree, and also the rels
 *    in the query so we can compare them in a later step.
 *
 */
static bool
mv_rewrite_join_node_is_valid_for_plan_recurse (PlannerInfo *root,
								  Node *node,
								  Query *parsed_mv_query,
								  Relids join_relids,
								  ListOf (Oid) **mv_oids_involved,
								  Relids *query_relids_involved,
								  RelOptInfo **query_found_rel,
								  ListOf (RestrictInfo *) **collated_query_quals,
								  ListOf (RestrictInfo *) **collated_mv_quals)
{
	if (g_trace_join_clause_check)
		elog(INFO, "%s: processing node: %s", __func__, nodeToString (node));
	
	if (IsA (node, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) node;
		RangeTblEntry *mv_rte = rt_fetch (rtr->rtindex, parsed_mv_query->rtable);
		
		// Find the OID it references.
		Oid mv_oid = mv_rte->relid;
		
		if (mv_rte->rtekind == RTE_RELATION)
			if (list_member_oid (*mv_oids_involved, mv_oid))
			{
				if (g_log_match_progress)
					elog(INFO, "%s: MV with multiple mentions of same OID not supported.", __func__);

				return false;
			}
		
		// Locate the same OID the query plan.
		for (int i = bms_next_member (join_relids, -1);
			 i >= 0;
			 i = bms_next_member (join_relids, i))
		{
			RangeTblEntry *plan_rte = planner_rt_fetch (i, root);
			
			if (g_trace_join_clause_check)
				elog(INFO, "%s: MV RTE: %s", __func__, nodeToString (mv_rte));
			if (g_trace_join_clause_check)
				elog(INFO, "%s: plan RTE: %s", __func__, nodeToString (plan_rte));
			
			if (mv_rte->rtekind == plan_rte->rtekind && plan_rte->rtekind == RTE_RELATION)
			{
				Oid join_oid = plan_rte->relid;

				// If found, keep a record so we know what joins to what later.
				if (mv_oid == join_oid)
				{
					*mv_oids_involved = list_append_unique_oid (*mv_oids_involved, mv_oid);
					*query_relids_involved = bms_add_member (*query_relids_involved, i);

					if (g_trace_join_clause_check)
						elog(INFO, "%s: match found: MV rtindex: %d; MV Oid: %d; matches plan relid: %d", __func__, rtr->rtindex, (int) mv_oid, i);

					RelOptInfo *rel = find_base_rel (root, i);
					if (rel->baserestrictinfo != NULL)
						*collated_query_quals = list_concat_unique_ptr (*collated_query_quals, rel->baserestrictinfo);

					if (query_found_rel != NULL)
						*query_found_rel = rel;
					
					return true;
				}
			}
			else if (mv_rte->rtekind == plan_rte->rtekind && plan_rte->rtekind == RTE_SUBQUERY)
			{
				if (equal_tree_walker (mv_rte, plan_rte, mv_rewrite_rte_equals_walker, NULL))
				{
					*mv_oids_involved = list_append_unique_oid (*mv_oids_involved, mv_oid);
					*query_relids_involved = bms_add_member (*query_relids_involved, i);
					
					if (g_trace_join_clause_check)
						elog(INFO, "%s: match found: MV rtindex: %d; MV Oid: %d; matches plan relid: %d", __func__, rtr->rtindex, (int) mv_oid, i);
					
					RelOptInfo *rel = find_base_rel (root, i);
					
					ListOf (RestrictInfo *) *pushed_quals = mv_rewrite_get_pushed_down_exprs (root, rel, (Index) i);
					*collated_query_quals = list_concat_unique_ptr (*collated_query_quals, pushed_quals);

					if (query_found_rel != NULL)
						*query_found_rel = rel;

					return true;
				}
			}
		}

		// Otherwise, since we can't find a match, that means the MV doesn't match the query.

		if (g_log_match_progress)
			elog(INFO, "%s: no match found in plan for MV rtindex: %d; MV Oid: %d", __func__, rtr->rtindex, (int) mv_oid);
		
		return false;
	}
	else if (IsA (node, JoinExpr))
	{
		JoinExpr *je = castNode (JoinExpr, node);
		
		Node *larg = je->larg;
		Node *rarg = je->rarg;
		
		Relids larg_query_relids = NULL;
		
		// Find the RelOptInfos in the plan that represents larg and rarg
		RelOptInfo *lrel, *rrel;

		// Check the left side of the join is legal according to the plan.
		if (!mv_rewrite_join_node_is_valid_for_plan_recurse (root, larg, parsed_mv_query, join_relids, mv_oids_involved, &larg_query_relids, &lrel, collated_query_quals, collated_mv_quals))
		{
			if (g_log_match_progress)
				elog(INFO, "%s: left side of join is not valid for plan: %s", __func__, nodeToString (larg));

			return false;
		}

		Relids rarg_query_relids = NULL;
		
		// Check the right side of the join is legal according to the plan.
		if (!mv_rewrite_join_node_is_valid_for_plan_recurse (root, rarg, parsed_mv_query, join_relids, mv_oids_involved, &rarg_query_relids, &rrel, collated_query_quals, collated_mv_quals))
		{
			if (g_log_match_progress)
				elog(INFO, "%s: right side of join is not valid for plan: %s", __func__, nodeToString (rarg));
			
			return false;
		}
		
		if (g_trace_join_clause_check)
			elog(INFO, "%s: checking if join between %s and %s is legal...", __func__, bmsToString (larg_query_relids), bmsToString (rarg_query_relids));

		Relids outrelids = bms_union (larg_query_relids, rarg_query_relids);
		
		// FIXME: Check the two lists don't overlap!
		
		// Give the two legal sides of the join, check it is itself legal according to the plan.
		bool reversed;
		SpecialJoinInfo *sjinfo;
		
		if (!join_is_legal (root, lrel, rrel, outrelids, &sjinfo, &reversed))
		{
			if (g_log_match_progress)
				elog(INFO, "%s: MV contains a join not matched in query: %s", __func__, nodeToString (node));
			
			return false;
		}
		
		// Then check the join type.
		JoinType legal_jt = JOIN_INNER;
		if (sjinfo != NULL)
			legal_jt = sjinfo->jointype;
		
		// ... being mindful that we might have things the wrong way around.
		if (reversed)
		{
			if (legal_jt == JOIN_LEFT)
				legal_jt = JOIN_RIGHT;
			else if (legal_jt == JOIN_RIGHT)
				legal_jt = JOIN_LEFT;
			else if (legal_jt == JOIN_UNIQUE_INNER)
				legal_jt = JOIN_UNIQUE_OUTER;
			else if (legal_jt == JOIN_UNIQUE_OUTER)
				legal_jt = JOIN_UNIQUE_INNER;
		}
	
		if (legal_jt != je->jointype)
		{
			if (g_log_match_progress)
				elog(INFO, "%s: MV contains a join of type not matched in query.", __func__);
				
			return false;
		}
		
		RelOptInfo *outerrel = find_join_rel (root, outrelids);
		
		// Get hold of the JOIN clauses that relate to this join in the query.
		*collated_query_quals = list_concat_unique_ptr (*collated_query_quals, outerrel->joininfo);
		// (The list in RelOptInfo doesn't include the implied ECs.)
		*collated_query_quals = list_concat_unique_ptr (*collated_query_quals, generate_join_implied_equalities (root, outrelids, rarg_query_relids, lrel));

		// Similarly record the quals from the MV jointree.
		if (je->quals != NULL)
			*collated_mv_quals = list_append_unique_ptr (*collated_mv_quals, je->quals);
		
		if (g_trace_join_clause_check)
			elog(INFO, "%s: join for relids found: %s", __func__, bmsToString (*query_relids_involved));
		
		*query_relids_involved = bms_union (*query_relids_involved, outrelids);
		
		if (query_found_rel != NULL)
			*query_found_rel = outerrel;

		return true;
	}
	else if (IsA (node, FromExpr))
	{
		FromExpr *fe = (FromExpr *) node;
		
		// Process joins, one by one...

		// Handle the special case of a list of one.
		if (list_length (fe->fromlist) == 1)
		{
			// Record the quals from the MV jointree.
			if (fe->quals != NULL)
				*collated_mv_quals = list_append_unique_ptr (*collated_mv_quals, fe->quals);

			return mv_rewrite_join_node_is_valid_for_plan_recurse (root, list_nth_node (Node, fe->fromlist, 0), parsed_mv_query, join_relids, mv_oids_involved, query_relids_involved, NULL, collated_query_quals, collated_mv_quals);
		}
		
		// Handle the special case of a list of two.
		if (list_length (fe->fromlist) == 2)
		{
			JoinExpr *je = makeNode (JoinExpr);
			je->jointype = JOIN_INNER; // always an inner join
			je->larg = list_nth_node (Node, fe->fromlist, 0);
			je->rarg = list_nth_node (Node, fe->fromlist, 1);
			je->quals = fe->quals;

			return mv_rewrite_join_node_is_valid_for_plan_recurse (root, (Node *) je, parsed_mv_query, join_relids, mv_oids_involved, query_relids_involved, NULL, collated_query_quals, collated_mv_quals);
		}

		// Note that it is possible that there are /no/ items in the FROM list (e.g.,
		// in case of UNION ALL).
		if (list_length (fe->fromlist) >= 2)
		{
			Relids outrelids = NULL;
			
			{
				// Fake JoinExpr node and create an innter join between successive parties
				JoinExpr *je = makeNode (JoinExpr);
				je->jointype = JOIN_INNER; // always an inner join
				
				bool first = true;
				ListCell *lc;
				foreach (lc, fe->fromlist)
				{
					Node *child = lfirst (lc);
	
					List *mv_oids_involved = NIL;
					
					if (!first)
						je->larg = je->rarg; // prior rarg becomes left
					
					je->rarg = child; // current child becomes right
					
					if (!first)
					{
						if (!mv_rewrite_join_node_is_valid_for_plan_recurse (root, (Node *) je, parsed_mv_query, join_relids, &mv_oids_involved, &outrelids, NULL, collated_query_quals, collated_mv_quals))
							// Since we can't find a match, the MV doesn't match
							return false;
						
						*query_relids_involved = bms_union (*query_relids_involved, outrelids);
					}
					
					first = false;
				}
			}
		
			RelOptInfo *outerrel = find_join_rel (root, outrelids);
			
			// Get hold of the JOIN clauses that relate to this join in the query.
			if (outerrel->joininfo != NIL)
				*collated_query_quals = list_concat_unique_ptr (*collated_query_quals, outerrel->joininfo);

			*query_relids_involved = bms_union (*query_relids_involved, outrelids);

			if (query_found_rel != NULL)
				*query_found_rel = outerrel;
		}
		
		// Record the quals from the MV jointree.
		if (fe->quals != NULL)
			*collated_mv_quals = list_append_unique_ptr (*collated_mv_quals, fe->quals);
		
		if (g_trace_join_clause_check)
			elog(INFO, "%s: join for relids found: %s", __func__, bmsToString (*query_relids_involved));
		
		return true;
	}

	elog (ERROR, "unrecognized node type: %d", (int) nodeTag (node));
	
	return false;
}

/**
 * Check the FROM/JOIN clauses of the plan. The rel supplied (grouped_rel and its input, input_rel)
 * must contain joins rels and join clauses that are at least a subset of those in the plan.
 *
 * We don't check WHERE clauses in this routine, and so any non-FROM/JOIN cluases we find may be
 * returned in the list of additional_where_clauses.
 */
static bool
mv_rewrite_from_join_clauses_are_valid_for_mv (PlannerInfo *root,
                                     Query *parsed_mv_query,
									 RelOptInfo *input_rel,
                                     RelOptInfo *grouped_rel,
                                     ListOf (RestrictInfo * or Expr *) **additional_where_clauses,
                                     struct mv_rewrite_xform_todo_list *todo_list)
{
	if (g_trace_join_clause_check)
		elog(INFO, "%s: grouped_rel: %s", __func__, nodeToString (grouped_rel));
    if (g_trace_join_clause_check)
        elog(INFO, "%s: input_rel: %s", __func__, nodeToString (input_rel));

    if (g_debug_join_clause_check)
        elog(INFO, "%s: MV jointree: %s", __func__, nodeToString (parsed_mv_query->jointree));
	
    ListOf (RestrictInfo * or Expr *) *additional_clauses = NIL;
	
	if (IS_UPPER_REL(grouped_rel))
	{
		if (IS_SIMPLE_REL (input_rel))
			additional_clauses = list_copy (input_rel->baserestrictinfo);
		
		else if (IS_JOIN_REL (input_rel))
		{
			// Check all the baserel OIDs in the JOIN exatly match the OIDs in the MV, and
			// that the every join in the MV is legal and correct according to the plan.
			if (!mv_rewrite_join_node_is_valid_for_plan (root, parsed_mv_query, input_rel, &additional_clauses))
				return false;
		}
	}
	else
		return false;
    
    if (g_debug_join_clause_check)
        elog(INFO, "%s: searching against these clauses in the query: %s", __func__, nodeToString (additional_clauses));
	
    // 4. The balance of clauses must now be treated as if they were part of the
    //    WHERE clause list. Return them so the WHERE clause processing can handle
    //    them appropriately.
    
    *additional_where_clauses = additional_clauses;

    if (g_debug_join_clause_check)
        elog(INFO, "%s: balance of clauses: %s", __func__, nodeToString (*additional_where_clauses));
    
    return true;
}

static void
mv_rewrite_process_having_clauses (PlannerInfo *root,
                        Query *parsed_mv_query,
                        RelOptInfo *grouped_rel,
                        ListOf (RestrictInfo * or Expr *) **additional_where_clauses,
                        struct mv_rewrite_xform_todo_list *todo_list)
{
    ListOf (Expr *) *havingQual = NIL;

    // FIXME: can we work around need to wrap the existing list in RIs?
    ListCell *lc;
    foreach(lc, (List *) root->parse->havingQual)
    {
        Expr       *expr = (Expr *) lfirst(lc);
        
        havingQual = lappend (havingQual, expr);
    }

    if (g_trace_having_clause_source_check)
        elog(INFO, "%s: clauses: %s", __func__, nodeToString (havingQual));
    
    // Having qualifiers may simply be appended to our working set of additional WHERE clauses:
    // they will be checked for validity against the MV tList later.
    
    *additional_where_clauses = list_concat (*additional_where_clauses, havingQual);
}

static bool
mv_rewrite_where_clauses_are_valid_for_mv (PlannerInfo *root,
                                               Query *parsed_mv_query,
											   RelOptInfo *input_rel,
                                               RelOptInfo *grouped_rel,
                                               ListOf (RestrictInfo * or Expr *) *query_where_clauses,
                                               List **transformed_clist_p,
                                               struct mv_rewrite_xform_todo_list *todo_list)
{
	// All WHERE clauses fo a JOIN_REL will have been elicited during processing of
	// the FROM/JOIN clauses of the input_rel.
	
    if (g_trace_where_clause_source_check)
        elog(INFO, "%s: clauses: %s", __func__, nodeToString (query_where_clauses));
	
    ListCell   *lc;
    foreach (lc, query_where_clauses)
    {
        Expr *expr = lfirst (lc);
        
        /* Extract clause from RestrictInfo, if required */
        if (IsA(expr, RestrictInfo))
        {
            expr = ((RestrictInfo *) expr)->clause;
        }
        
        if (!mv_rewrite_check_expr_targets_in_mv_tlist (root, parsed_mv_query, (Node *) expr, todo_list, g_trace_where_clause_source_check))
        {
            if (g_log_match_progress)
                elog(INFO, "%s: WHERE clause (%s) not found in MV SELECT list", __func__, nodeToString(expr));
            
            return false;
        }
        
        *transformed_clist_p = lappend (*transformed_clist_p, expr);
    }
    
    return true;
}

/**
 * Determine whether all the expressions selected in the supplied target list
 * are su supported by clauses in the MV's target list.
 *
 * Returns true if they are; false otherwise.
 *
 * If a match is found, the todo_list is updated in order to (later) transform
 * the relevant part of the expression in the seleted target list into a VAR
 * node that references a column in the replacement MV SELECT list.
 */
static bool
mv_rewrite_select_clauses_are_valid_for_mv (PlannerInfo *root,
                                                Query *parsed_mv_query,
                                                RelOptInfo *grouped_rel,
                                                ListOf (TargetEntry *) *selected_tlist,
                                                struct mv_rewrite_xform_todo_list *todo_list)
{
	// One by one, check each target in the supplied target list. Each target
	// must be supported by a matching cluase in the MV's target list.
	
    ListCell   *lc;
    foreach (lc, selected_tlist)
    {
        TargetEntry *tle = lfirst(lc);
        
        //elog(INFO, "%s: matching expr: %s", __func__, nodeToString (tle));
        
        if (!mv_rewrite_check_expr_targets_in_mv_tlist (root, parsed_mv_query, (Node *) tle, todo_list, g_trace_select_clause_source_check))
        {
            if (g_log_match_progress)
                elog(INFO, "%s: expr (%s) not found in MV tlist", __func__, mv_rewrite_deparse_expression (root->parse->rtable, tle->expr));
            
            return false;
        }
    }
	
	// It is acceptable for the MV to contain other entries in its target list:
	// its just that we won't pull them in the replacement scan.
    
    // Complete match found!
    return true;
}

static Query *
mv_rewrite_build_mv_scan_query (Oid matviewOid,
								StringInfo mv_name,
								ListOf (Value *) *colnames,
								List *mv_scan_quals,
								List *transformed_tlist)
{
	Query *mv_scan_query = makeNode (Query);

	mv_scan_query->commandType = 1;
	mv_scan_query->targetList = transformed_tlist;

	RangeTblEntry *mvsqrte = makeNode (RangeTblEntry);
	mvsqrte->relid = matviewOid;
	mvsqrte->inh = true;
	mvsqrte->inFromCl = true;
	mvsqrte->requiredPerms = ACL_SELECT;
	mvsqrte->selectedCols = 0; // FIXME: TODO
	mvsqrte->relkind = RELKIND_MATVIEW;
	mvsqrte->eref = makeNode (Alias);
	mvsqrte->eref->aliasname = mv_name->data;
	mvsqrte->eref->colnames = colnames;

	mv_scan_query->rtable = list_make1 (mvsqrte);

	mv_scan_query->jointree = makeNode (FromExpr);

	RangeTblRef *mvsrtr = makeNode (RangeTblRef);
	mvsrtr->rtindex = 1;

	mv_scan_query->jointree->fromlist = list_make1 (mvsrtr);
	
	if (mv_scan_quals != NULL)
	{
		Expr *quals_expr;
		
		if (list_length (mv_scan_quals) > 1)
		{
			quals_expr = (Expr *) makeNode (BoolExpr);
			
			BoolExpr *be = (BoolExpr *) quals_expr;
			
			be->boolop = AND_EXPR;
			be->location = -1;
			be->args = mv_scan_quals;
		}
		else
			quals_expr = (Expr *) list_nth (mv_scan_quals, 0);
		
		mv_scan_query->jointree->quals = (Node *) quals_expr;
	}
	
	return mv_scan_query;
}

static bool
mv_rewrite_evaluate_mv_for_rewrite (PlannerInfo *root,
							  RelOptInfo *input_rel,
                              RelOptInfo *grouped_rel,
                              List *grouped_tlist,
                              StringInfo mv_name, StringInfo mv_schema,
                              Query **alternative_query)
{
    ListOf (Expr *) *transformed_clist = NIL;
    ListOf (TargetEntry *) *transformed_tlist = grouped_tlist;
    
    struct mv_rewrite_xform_todo_list transform_todo_list = {
		.replaced_expr = NIL,
		.replacement_var = NIL,
		.replacement_varno = 1
	};
    
	Oid matviewOid;
	
	Query *parsed_mv_query;
	mv_rewrite_get_mv_query (grouped_rel, (const char *) mv_name->data, &parsed_mv_query, &matviewOid);
    
    // 1. Check the GROUP BY clause: it must match exactly.
    if (!mv_rewrite_check_group_clauses_for_mv(root, parsed_mv_query, grouped_rel, transformed_tlist, &transform_todo_list))
        return false;
    
    // FIXME: the above check only checks some selected clauses; the balance of
    // non-GROUPed columns would need to be re-aggregated by the outer, hence the above
    // check needs to be exact set equality.
    
    ListOf (RestrictInfo * or Expr *) *additional_where_clauses = NIL;
    
    // 2. Check the FROM and WHERE clauses: they must match exactly.
    if (!mv_rewrite_from_join_clauses_are_valid_for_mv (root, parsed_mv_query, input_rel, grouped_rel, &additional_where_clauses,
                                              &transform_todo_list))
        return false;
    
    // 3. Stage he HAVING clauses in the additional WHERE clause list. (They will actually be
    //    evaluated as part of the WHERE clause procesing.)
    mv_rewrite_process_having_clauses (root, parsed_mv_query, grouped_rel, &additional_where_clauses, &transform_todo_list);
    
    // 4. Check the additional WHERE clauses list, and any WHERE clauses not found in the FROM/JOIN list
    if (!mv_rewrite_where_clauses_are_valid_for_mv(root, parsed_mv_query, input_rel, grouped_rel, additional_where_clauses,
                                                       &transformed_clist, &transform_todo_list))
        return false;
    
    // 5. Check the SELECT clauses: they must be a subset of the MV's tList
    if (!mv_rewrite_select_clauses_are_valid_for_mv (root, parsed_mv_query, grouped_rel, grouped_tlist, &transform_todo_list))
        return false;
    
    // 6. Transform Exprs that were found to match Exprs in the MV into Vars that instead reference MV tList entries
    transformed_tlist = mv_rewrite_xform_todos (transformed_tlist, &transform_todo_list);
	List *quals = mv_rewrite_xform_todos (transformed_clist, &transform_todo_list);
	
    // 7. Build the alternative query.
	*alternative_query = mv_rewrite_build_mv_scan_query (matviewOid, mv_name,
														 mv_rewrite_get_query_tlist_colnames (parsed_mv_query),
														 quals, transformed_tlist);
	
    return true;
}

static CustomExecMethods mv_rewrite_exec_methods = {
	.CustomName = "MVRewriteScan",

	/* Required executor methods */
	.BeginCustomScan = mv_rewrite_begin_scan,
	.ExecCustomScan = mv_rewrite_exec_scan,
	.EndCustomScan = mv_rewrite_end_scan,
	.ReScanCustomScan = mv_rewrite_rescan_scan,

	/* Optional: print additional information in EXPLAIN */
	.ExplainCustomScan = mv_rewrite_explain_scan
};

/**
 * Allocate a CustomScanState for this CustomScan. The actual allocation will
 * often be larger than required for an ordinary CustomScanState, because many
 * providers will wish to embed that as the first field of a larger structure.
 * The value returned must have the node tag and methods set appropriately, but
 * other fields should be left as zeroes at this stage; after
 * ExecInitCustomScan performs basic initialization, the BeginCustomScan
 * callback will be invoked to give the custom scan provider a chance to do
 * whatever else is needed.
 */
static Node *
mv_rewrite_create_scan_state (CustomScan *cscan)
{
	mv_rewrite_custom_scan_state *css =
		(mv_rewrite_custom_scan_state *) newNode (sizeof (mv_rewrite_custom_scan_state), T_CustomScanState);

	css->sstate.methods = &mv_rewrite_exec_methods;

	PlannedStmt *pstmt = list_nth_node (PlannedStmt, cscan->custom_private, 0);

	css->pstmt = pstmt;
	css->query = list_nth_node (Value, cscan->custom_private, 1);
	css->competing_cost = list_nth_node (Value, cscan->custom_private, 2);

	return (Node *) css;
}

static CustomScanMethods mv_rewrite_scan_methods = {
	.CustomName = "MVRewriteScan",
	.CreateCustomScanState = mv_rewrite_create_scan_state
};

/**
 * Convert a custom path to a finished plan. The return value will generally be
 * a CustomScan object, which the callback must allocate and initialize. See
 * Section 58.2 for more details.
 */
static Plan *
mv_rewrite_plan_mv_rewrite_path (PlannerInfo *root,
				   RelOptInfo *rel,
				   CustomPath *best_path,
				   List *tlist,
				   List *clauses,
				   List *custom_plans)
{
	PlannedStmt *pstmt = list_nth_node (PlannedStmt, best_path->custom_private, 0);
	ListOf (TargetEntry *) *grouped_tlist = list_nth_node (List, best_path->custom_private, 1);
	Relids grouped_relids = list_nth (best_path->custom_private, 2);
	Value *query = list_nth_node (Value, best_path->custom_private, 3);
	Value *competing_cost = list_nth_node (Value, best_path->custom_private, 4);

	CustomScan *cscan = makeNode (CustomScan);

	cscan->flags = 0;
	cscan->custom_private = list_make3 (pstmt, query, competing_cost);
	cscan->custom_exprs = NIL;
	cscan->custom_scan_tlist = grouped_tlist;
	cscan->scan.plan.targetlist = grouped_tlist;
	cscan->custom_plans = pstmt->subplans;
	cscan->custom_relids = grouped_relids;

	cscan->methods = &mv_rewrite_scan_methods;

	return (Plan *) cscan;
}

static CustomPathMethods mv_rewrite_path_methods = {
	.CustomName = "MVRewritePath",
	.PlanCustomPath = mv_rewrite_plan_mv_rewrite_path
};

static bool
mv_rewrite_involved_rels_enabled_for_rewrite (List *involved_rel_names)
{
	// If no list is configured, default to enabling rewrite for any relation.
	if (g_rewrite_enabled_for_tables == NULL)
		return true;
	
	ListOf (char *) *enabled_tables;
	
	if (!SplitIdentifierString (g_rewrite_enabled_for_tables, ',', &enabled_tables))
	{
		// There could be a problem with the enabled table list. Default to caution.
		return false;
	}
		
	// Attempt to locate every involved table in the configured list of enabled-for-rewrite tables.
	ListCell *lc;
	foreach (lc, involved_rel_names)
	{
		const char *involved_rel_name = lfirst (lc);
		
		bool found = false;
		
		ListCell *lc2;
		foreach (lc2, enabled_tables)
		{
			const char *enabled_table = lfirst (lc2);
			
			if (strcmp (involved_rel_name, enabled_table) == 0)
			{
				found = true;
				break;
			}
		}
		
		// If even a single table is not matched, then we must reject.
		if (!found)
			return false;
	}
	
	// All tables found; good.
	return true;
}

static Path *
mv_rewrite_create_mv_scan_path (PlannerInfo *root,
								Query *alternative_query,
								RelOptInfo *grouped_rel,
								List *grouped_tlist,
								PathTarget *grouping_target,
								StringInfo mv_name, StringInfo mv_schema)
{
	/* plan_params should not be in use in current query level */
	if (root->plan_params != NIL)
	{
		elog(ERROR, "%s: plan_params != NIL", __func__);
		return NULL;
	}
	
	/* Generate Paths for the subquery. */
	PlannedStmt *pstmt = pg_plan_query (alternative_query,
										(root->glob->parallelModeOK ? CURSOR_OPT_PARALLEL_OK : 0 )
										| (root->tuple_fraction <= 1e-10 ? CURSOR_OPT_FAST_PLAN : 0),
										root->glob->boundParams);
	
	Plan *subplan = pstmt->planTree;
	
	/* Bury our subquery inside a CustomPath. We need to do this because
	 * the SubQueryScanPath code expects the query to be 1:1 related to the
	 * root's PlannerInfo --- now, of course, the origingal query didn't have
	 * any such subquery in it at all, let alone with a 1:1 relationship.
	 */
	CustomPath *cp = makeNode (CustomPath);
	cp->path.pathtype = T_CustomScan;
	cp->flags = 0;
	cp->methods = &mv_rewrite_path_methods;
	
	// Take the cost information from the replacement query...
	cp->path.startup_cost = subplan->startup_cost;
	cp->path.total_cost = subplan->total_cost;
	cp->path.rows = subplan->plan_rows;
	if (IsA (subplan, Gather))
		cp->path.parallel_workers = ((Gather *) subplan)->num_workers;
	else if (IsA (subplan, GatherMerge))
		cp->path.parallel_workers = ((GatherMerge *) subplan)->num_workers;
	
	// The target that the Path will delivr is our grouped_rel/grouping_target
	cp->path.pathtarget = grouping_target;
	cp->path.parent = grouped_rel;
	
	cp->path.param_info = NULL;
	cp->path.parallel_aware = subplan->parallel_aware;
	cp->path.parallel_safe = subplan->parallel_safe;
	
	// Work out the cost of the competing (unrewritten) plan...
	StringInfo competing_cost = makeStringInfo();

	ListCell *lc;
	foreach (lc, grouped_rel->pathlist)
	{
		Path *p = lfirst_node (Path, lc);
		
		if (competing_cost->len > 0)
			appendStringInfo (competing_cost, "; ");
		
		appendStringInfo (competing_cost, "cost=%.2f..%.2f rows=%.0f width=%d",
						  p->startup_cost, p->total_cost, p->rows, p->pathtarget->width);
	}
	
	StringInfo alt_query_text = makeStringInfo();
	appendStringInfo (alt_query_text, "scan of %s.%s", mv_schema->data, mv_name->data);
		
	//List	   *rtable = alternative_query->rtable;
	//List	   *rtable_names = select_rtable_names_for_explain (rtable, bms_make_singleton (1));
	//ListOf (deparse_namespace) *deparse_cxt = deparse_context_for_plan_rtable (rtable, rtable_names);
	
	// FIXME: include at least the additional WHERE clauses in the alternative query text
	
	cp->custom_private = list_make5 (pstmt, grouped_tlist, grouped_rel->relids,
									 makeString (alt_query_text->data), makeString (competing_cost->data));
	
	return (Path *) cp;
}

static void
mv_rewrite_add_rewritten_mv_paths (PlannerInfo *root,
                             RelOptInfo *input_rel, // if grouping
                             RelOptInfo *inner_rel, RelOptInfo *outer_rel, // if joining
                             RelOptInfo *grouped_rel,
                             PathTarget *grouping_target)
{
    // Find the set of rels involved in the query being planned...
	ListOf (char *) *involved_rel_names = mv_rewrite_get_involved_rel_names (root, inner_rel, input_rel, outer_rel);

	// Check first that all of those rels are enabled for query rewrite...
	if (!mv_rewrite_involved_rels_enabled_for_rewrite (involved_rel_names))
	{
		if (g_log_match_progress)
			elog(INFO, "%s: MV rewrite not enabled for one or more table in the query.", __func__);

		return;
	}

	// See if there are any candidate MVs...
	List *mvs_schema, *mvs_name;
    mv_rewrite_find_related_mvs_for_rel (involved_rel_names, &mvs_schema, &mvs_name);
	
	if (mvs_name == NULL ||
		list_length (mvs_name) == 0)
	{
		if (g_log_match_progress)
		{
			StringInfo involved_rel_string = makeStringInfo();
			
			ListCell *lc;
			foreach (lc, involved_rel_names)
			{
				const char *involved_rel_name = lfirst (lc);
				
				if (involved_rel_string->len > 0)
					appendStringInfoString (involved_rel_string, ",");
				
				appendStringInfoString (involved_rel_string, involved_rel_name);
			}
			
			elog(INFO, "%s: no candidate MVs for query involving {%s}.", __func__, involved_rel_string->data);
		}

		return;
	}
    
    ListOf (TargetEntry *) *grouped_tlist = make_tlist_from_pathtarget (root->upper_targets[UPPERREL_GROUP_AGG]);
	
    // Evaluate each MV in turn...
    ListCell   *sc, *nc;
    forboth (sc, mvs_schema, nc, mvs_name)
    {
        StringInfo mv_schema = lfirst(sc), mv_name = lfirst(nc);
        
        if (g_log_match_progress)
            elog(INFO, "%s: evaluating MV: %s.%s", __func__, mv_schema->data, mv_name->data);

        Query *alternative_query;
        
		if (!mv_rewrite_evaluate_mv_for_rewrite (root, input_rel, grouped_rel, grouped_tlist, mv_name, mv_schema, &alternative_query))
            continue;
        
        // 8. Finally, create and add the path.
        if (g_log_match_progress)
            elog(INFO, "%s: creating and adding path for alternate query: %s", __func__, nodeToString (alternative_query));
        
        // Add generated path into grouped_rel.
        add_path (grouped_rel,
				  mv_rewrite_create_mv_scan_path (root, alternative_query, grouped_rel, grouped_tlist, grouping_target, mv_name, mv_schema));

        if (g_log_match_progress)
            elog(INFO, "%s: path added.", __func__);
        
        return;
    }
}

