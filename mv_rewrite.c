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
#include "pg_config.h"

#include "mv_rewrite.h"
#include "equalwalker.h"
#include "extension.h"
#include "join_is_legal.h"
#include "build_joinrel_restrictlist.h"
#include "release_rewrite_locks.h"

#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
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
#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#else
#include "optimizer/optimizer.h"
#endif
#include "optimizer/tlist.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
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
#include "utils/fmgroids.h"

PG_MODULE_MAGIC;

#define elog_if(cond, ...) if ((cond)) elog (__VA_ARGS__);

#define ListOf(type) List /* type */

typedef signed int RelationKind;
#define REL_JOIN (-1)

static const unsigned int indent = 2;
static const char *log_prefix = "";

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
	pfree (self);
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
	
	ExplainState *my_es = palloc (sizeof (ExplainState));
	
	// Copy the Explain setup in order that we don't disturb the outer state.
	// (We do this because ExplainNode gets confused to
	// see a fully formed query plan in a subquery. Nor really sure why...)
	*my_es = *es;
	
	// Reset other state...
	my_es->grouping_stack = NULL;
	my_es->pstmt = NULL;
	my_es->rtable = NULL;
	my_es->rtable_names = NULL;
	my_es->deparse_cxt = NULL;
	my_es->printed_subplans = NULL;
	
	// Explain the MV scan.
	ExplainPrintPlan (my_es, sstate->qdesc);
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
	if (root->hasLateralRTEs)
		return false;
	
	if (root->parse->hasDistinctOn)
		return false;

	// Default is that we _do_ support rewrite.
	return true;
}

static void
mv_rewrite_add_rewritten_mv_paths (PlannerInfo *root,
								   RelationKind stage,
								   RelOptInfo *input_rel,
								   RelOptInfo *grouped_rel,
								   PathTarget *grouping_target,
								   unsigned int log_indent);

void
mv_rewrite_set_join_pathlist_hook (PlannerInfo *root,
								   RelOptInfo *join_rel,
								   RelOptInfo *outerrel,
								   RelOptInfo *innerrel,
								   JoinType jointype,
								   JoinPathExtraData *extra)
{
	const unsigned int log_indent = 0;
	
	// Delegate first to any other extensions if they are already hooked.
	if (next_set_join_pathlist_hook)
		(*next_set_join_pathlist_hook) (root, join_rel, outerrel, innerrel, jointype, extra);

	// Evaluate the query being planned for any unsupported features.
	if (!mv_rewrite_eval_query_for_rewrite_support (root))
	{
		elog_if (g_log_match_progress, INFO, "%*squery requires unsupported features.", log_indent, log_prefix);
		
		return;
	}
	
	if (g_rewrite_minimum_cost > 0.0)
	{
		// We need to first identify the cheapest cost paths from the set of possible paths.
		// (The planner would normally call this a little later.)
		set_cheapest (join_rel);

		if (join_rel->cheapest_total_path != NULL)
			if (join_rel->cheapest_total_path->total_cost < g_rewrite_minimum_cost)
			{
				elog_if (g_log_match_progress, INFO, "%*salready have path with acceptable cost.", log_indent, log_prefix);
				
				return;
			}
	}
	
	elog_if (g_trace_match_progress, INFO, "%*sroot: %s", log_indent, log_prefix, nodeToString (root));
	elog_if (g_trace_match_progress, INFO, "%*sjoinrel: %s", log_indent, log_prefix, nodeToString (join_rel));
	
	// If we can rewrite, add those alternate paths...
	mv_rewrite_add_rewritten_mv_paths (root, REL_JOIN, NULL, join_rel, join_rel->reltarget, log_indent);
}

/*
 * mv_rewrite_create_upper_paths_hook
 *        Add paths for post-join operations like aggregation, grouping etc.
 *
 */
void
mv_rewrite_create_upper_paths_hook(PlannerInfo *root,
			      UpperRelationKind stage,
			      RelOptInfo *input_rel,
			      RelOptInfo *upper_rel
#if PG_VERSION_NUM >= 110000
			      , void *extra
#endif
)
{
	const unsigned int log_indent = 0;
	
	// Delegate first to any other extensions if they are already hooked.
	if (next_create_upper_paths_hook)
		(*next_create_upper_paths_hook) (root, stage, input_rel, upper_rel
#if PG_VERSION_NUM >= 110000
						 , extra
#endif
						 );

	// Ignore stages we don't support.
	switch (stage)
	{
		case UPPERREL_SETOP:
		case UPPERREL_GROUP_AGG:
		case UPPERREL_DISTINCT:
		case UPPERREL_ORDERED:
		case UPPERREL_FINAL:
			break;
			
		default:
			elog_if (g_trace_match_progress, INFO, "%*supper path stage (%d) not supported.", log_indent, log_prefix, stage);
			return;
	}
	
	// Evaluate the query being planned for any unsupported features.
	if (!mv_rewrite_eval_query_for_rewrite_support (root))
	{
		elog_if (g_log_match_progress, INFO, "%*squery requires unsupported features.", log_indent, log_prefix);
		
		return;
	}
	
	if (g_rewrite_minimum_cost > 0.0)
	{
		// We need to first identify the cheapest cost paths from the set of possible paths.
		// (The planner would normally call this a little later.)
		set_cheapest (upper_rel);
		
		if (upper_rel->cheapest_total_path != NULL)
			if (upper_rel->cheapest_total_path->total_cost < g_rewrite_minimum_cost)
			{
				elog_if (g_log_match_progress, INFO, "%*salready have path with acceptable cost.", log_indent, log_prefix);

				return;
			}
	}
	
	elog_if (g_trace_match_progress, INFO, "%*sstage: %d", log_indent, log_prefix, stage);
	elog_if (g_trace_match_progress, INFO, "%*sroot: %s", log_indent, log_prefix, nodeToString (root));
	elog_if (g_trace_match_progress, INFO, "%*sinput_rel: %s", log_indent, log_prefix, nodeToString (input_rel));
	elog_if (g_trace_match_progress, INFO, "%*supper_rel: %s", log_indent, log_prefix, nodeToString (upper_rel));

	// If we can rewrite, add those alternate paths...
	
	PathTarget *upper_target = root->upper_targets[stage];
	if (upper_target == NULL)
		upper_target = upper_rel->reltarget;

#if PG_VERSION_NUM < 120000
	// Prior to PG11, the upper_target for an UPPERREL_ORDERED and
	// UPPERREL_DISTINCT was not stored.
	if (upper_target == NULL)
	{
		if (stage == UPPERREL_ORDERED)
			upper_target = root->upper_targets[UPPERREL_FINAL];
		else if (stage == UPPERREL_DISTINCT)
			upper_target = root->upper_targets[UPPERREL_WINDOW];
		else
			elog (ERROR, "UPPERREL path taget not found.");
	}
#endif

	mv_rewrite_add_rewritten_mv_paths (root, (RelationKind) stage, input_rel, upper_rel, upper_target, log_indent);
}

/*
 * Construct the all_baserels Relids set.
 *
 * This is sometimes needed when root->all_baserels is either not yet set, or will never
 * become set.
 */
static Relids
mv_rewrite_find_all_baserels (PlannerInfo *root)
{
	/*
	 * Construct the all_baserels Relids set.
	 */
	Relids all_baserels = NULL;
	for (Index rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];
		
		/* there may be empty slots corresponding to non-baserel RTEs */
		if (brel == NULL)
			continue;
		
		Assert(brel->relid == rti); /* sanity check on array */
		
		/* ignore RTEs that are "other rels" */
		if (brel->reloptkind != RELOPT_BASEREL)
			continue;
		
		all_baserels = bms_add_member(all_baserels, (int) brel->relid);
	}

	return all_baserels;
}

CommonTableExpr *
mv_rewrite_get_cte_for_plan_rte (PlannerInfo *root, RangeTblEntry *rte, PlannerInfo **cteroot, Index *ndx)
{
	Index levelsup = rte->ctelevelsup;
	*cteroot = root;
	while (levelsup-- > 0)
	{
		*cteroot = (*cteroot)->parent_root;
		// We must be cautious: we are crawling widely over planner data
		// structures, but the plan may not yet be complete.
		if (!*cteroot)			/* shouldn't happen */
		{
			elog_if (g_trace_involved_rels_search, WARNING, "bad levelsup for CTE \"%s\"", rte->ctename);
			return NULL;
		}
	}
	
	/*
	 * Note: cte_plan_ids can be shorter than cteList, if we are still working
	 * on planning the CTEs (ie, this is a side-reference from another CTE).
	 * So we mustn't use forboth here.
	 */
	ListCell *lc;
	CommonTableExpr *cte = NULL;
	*ndx = 0;
	foreach(lc, (*cteroot)->parse->cteList)
	{
		cte = (CommonTableExpr *) lfirst(lc);
		
		if (strcmp(cte->ctename, rte->ctename) == 0)
			break;

		(*ndx)++;
	}
	
	// We must be cautious: we are crawling widely over planner data
	// structures, but the plan may not yet be complete.
	if (lc == NULL)				/* shouldn't happen */
	{
		elog_if (g_trace_involved_rels_search, WARNING, "could not find CTE \"%s\"", rte->ctename);
		return NULL;
	}
	// We must be cautious: we are crawling widely over planner data
	// structures, but the plan may not yet be complete.
	if (*ndx >= list_length((*cteroot)->cte_plan_ids))
	{
		elog_if (g_trace_involved_rels_search, WARNING, "could not find plan for CTE \"%s\"", rte->ctename);
		return NULL;
	}

	return cte;
}

static bool
mv_rewrite_find_oids_for_relids_recurse (PlannerInfo *root, Bitmapset *initial_relids, List **out_relids, ListOf (RelOptInfo *) **seen_ris);

static bool
mv_rewrite_find_oids_for_ri_recurse (PlannerInfo *root,
									 RelOptInfo *ri,
									 List **out_relids, ListOf (RelOptInfo *) **seen_ris)
{
	if (list_member_ptr (*seen_ris, ri))
		return true;
	
	*seen_ris = list_append_unique_ptr (*seen_ris, ri);
	
    if (ri->rtekind == RTE_RELATION)
    {
		*out_relids = list_append_unique_oid (*out_relids, planner_rt_fetch ((int) ri->relid, root)->relid);
		return true;
    }
	else if (ri->rtekind == RTE_SUBQUERY)
	{
		PlannerInfo *subroot = ri->subroot;
		
		// We must be cautious: we are crawling widely over planner data
		// structures, but the plan may not yet be complete.
		if (subroot == NULL)
		{
			elog_if (g_trace_involved_rels_search, WARNING, "%s: subquery rel with no subroot: %s", __func__, nodeToString (ri));
			return false;
		}
		
		if (!mv_rewrite_find_oids_for_relids_recurse (subroot,
													  subroot->all_baserels != NULL ? subroot->all_baserels : mv_rewrite_find_all_baserels (subroot),
													  out_relids, seen_ris))
			return false;
		
		return true;
	}
	else if (ri->rtekind == RTE_CTE)
	{
		Index ctendx;
		PlannerInfo *cteroot;

		CommonTableExpr *cte = mv_rewrite_get_cte_for_plan_rte (root, planner_rt_fetch ((int) ri->relid, root), &cteroot, &ctendx);
		
		// We must be cautious: we are crawling widely over planner data
		// structures, but the plan may not yet be complete.
		if (cte == NULL)
			return false;

		PlannerInfo *cte_subroot = list_nth (cteroot->glob->subroots, (int) ctendx);

		if (!mv_rewrite_find_oids_for_relids_recurse (cte_subroot,
									cte_subroot->all_baserels != NULL ? cte_subroot->all_baserels : mv_rewrite_find_all_baserels (cte_subroot),
									out_relids, seen_ris))
			return false;
		
		return true;
	}
	
	elog_if (g_trace_involved_rels_search, INFO, "%s: unsupported rtekind (%d): %s", __func__, ri->rtekind, nodeToString (ri));
	return false;
}

static bool
mv_rewrite_find_oids_for_relids_recurse (PlannerInfo *root, Bitmapset *initial_relids, List **out_relids, ListOf (RelOptInfo *) **seen_ris)
{
	bool rc = false;
	
	for (int x = bms_next_member (initial_relids, -1); x >= 0; x = bms_next_member (initial_relids, x))
	{
		RelOptInfo *ri = find_base_rel (root, x);
		
		if (!mv_rewrite_find_oids_for_ri_recurse (root, ri, out_relids, seen_ris))
			return false;
		
		rc = true;
	}
	
	return rc;
}

static void
mv_rewrite_append_relname_for_oid (Oid oid, StringInfo out)
{
	/*
	 * Core code already has some lock on each rel being planned, so we
	 * can use NoLock here.
	 */
	Relation	rel = heap_open (oid, NoLock);
	
	heap_close (rel, NoLock);
	
	const char *nspname = get_namespace_name (RelationGetNamespace(rel));
	const char *relname = RelationGetRelationName(rel);
	
	appendStringInfo (out, "%s.%s",
					  quote_identifier(nspname), quote_identifier(relname));
}

static bool
mv_rewrite_get_involved_rel_names (PlannerInfo *root,
								   RelationKind stage,
								   RelOptInfo *rel,
								   ListOf (char *) **involved_rel_names)
{
	List *rel_oids = NIL;

	ListOf (RelOptInfo *) *seen_ris = NIL;
	if (!mv_rewrite_find_oids_for_relids_recurse (root, IS_UPPER_REL (rel) ? root->all_baserels : rel->relids, &rel_oids, &seen_ris))
		return false;
	
	elog_if (g_trace_involved_rels_search, INFO, "%s: involved OIDs: %s", __func__, nodeToString (rel_oids));
	
	ListOf (char *) *tables_strlist = NIL;
	ListCell *lc;
	foreach (lc, rel_oids)
	{
		StringInfo table_name = makeStringInfo();
		
		mv_rewrite_append_relname_for_oid (lfirst_oid(lc), table_name);
		
		elog_if (g_trace_match_progress, INFO, "%s: query involves: %s", __func__, table_name->data);

		tables_strlist = lappend (tables_strlist, table_name->data);
	}

	*involved_rel_names = tables_strlist;

	return true;
}

static ListOf (Value (String)) *
mv_rewrite_find_related_mvs_for_rel (ListOf (char *) *involved_rel_names)
{
	const char *find_mvs_query = "SELECT v.schemaname || '.' || v.matviewname"
							" FROM "
							"   mv_rewrite.pgx_rewritable_matviews j, pg_catalog.pg_matviews v"
							" WHERE "
							" j.matviewschemaname = v.schemaname"
							" AND j.matviewname = v.matviewname"
							" AND j.tables @> $1";
	
    ArrayType /* text */ *tables_arr = strlist_to_textarray (involved_rel_names);
    
    Oid argtypes[] = { TEXTARRAYOID };
    Datum args[] = { PointerGetDatum (tables_arr) };

	bool old_rewrite_enabled = g_rewrite_enabled;
	g_rewrite_enabled = false;
	
    // We must be careful to manage the SPI memory context.
    
    MemoryContext mainCtx = CurrentMemoryContext;
	
	ListOf (StringInfo) *mvs_name = NIL;
	
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(WARNING, "%s: SPI_connect() failed", __func__);
        goto done; // (an empty set of matviews)
    }
    
    SPIPlanPtr plan = SPI_prepare (find_mvs_query, 1, argtypes);
    
    if (plan == NULL) {
        elog(WARNING, "%s: SPI_connect() failed", __func__);
        goto finish_done; // (an empty set of matviews)
    }
    
    Portal port = SPI_cursor_open (NULL, plan, args, NULL, true);
    
    for (SPI_cursor_fetch (port, true, 1);
         SPI_processed >= 1;
         SPI_cursor_fetch (port, true, 1))
    {
        MemoryContext spiCtx = MemoryContextSwitchTo (mainCtx);
		
		Value *name = makeString (SPI_getvalue (SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

        mvs_name = lappend (mvs_name, name);
        
        MemoryContextSwitchTo (spiCtx);
    }

    SPI_cursor_close (port);

finish_done:
    SPI_finish();
	
done:
	g_rewrite_enabled = old_rewrite_enabled;
	
	return mvs_name;
}

static Node *
mv_rewrite_eval_const_expressions_etm (Node *node, void *context)
{
	return eval_const_expressions (NULL, node);
}

static void
mv_rewrite_eval_const_expressions (Query *mv_query)
{
	ListCell *lc;
	
	mv_query->targetList = (List *) expression_tree_mutator ((Node *) mv_query->targetList, mv_rewrite_eval_const_expressions_etm, NULL);
	mv_query->limitCount = eval_const_expressions (NULL, mv_query->limitCount); //FIXME: should check it during eval
	mv_query->limitOffset = eval_const_expressions (NULL, mv_query->limitOffset); //FIXME: should check it during eval
	mv_query->jointree = (FromExpr *) expression_tree_mutator ((Node *) mv_query->jointree, mv_rewrite_eval_const_expressions_etm, NULL);

	foreach (lc, mv_query->cteList)
	{
		CommonTableExpr *mv_cte = lfirst (lc);
		mv_rewrite_eval_const_expressions ((Query *) mv_cte->ctequery);
	}

	// mv_query->groupingSets
	// mv_query->havingQual;
	// mv_query->windowClause;

	foreach (lc, mv_query->rtable)
	{
		RangeTblEntry *rte = lfirst (lc);
		if (rte->rtekind == RTE_SUBQUERY)
			mv_rewrite_eval_const_expressions (rte->subquery);
	}
}

static Query *
mv_rewrite_rewrite_mv_query (Query *query)
{
	elog_if (g_trace_parse_select_query, INFO, "%s: parse tree: %s", __func__, nodeToString (query));
	
	/* Lock and rewrite, using a copy to preserve the original query. */
	Query *copied_query = (Query *) copyObject (query);
	AcquireRewriteLocks (copied_query, true, false);
	ListOf (Query *) *rewritten = QueryRewrite (copied_query);
	
	/* SELECT should never rewrite to more or less than one SELECT query */
	elog_if (list_length (rewritten) != 1,  ERROR, "unexpected rewrite result");
	query = (Query *) linitial (rewritten);

	// Simplify constants in the target list because the raw rewrittten parse trees won't otherwise
	// match the expression trees that the planner shows us to match against.
	mv_rewrite_eval_const_expressions (query);
	
	elog_if (g_trace_parse_select_query, INFO, "%s: query: %s", __func__, nodeToString (query));
	
	return query;
}

static void
mv_rewrite_get_mv_query (const char *mv_name,
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
    elog_if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW, ERROR, "\"%s\" is not a materialized view", RelationGetRelationName(matviewRel));

    /* We don't allow an oid column for a materialized view. */
    Assert(!matviewRel->rd_rel->relhasoids);
    
    /*
     * Check that everything is correct for a refresh. Problems at this point
     * are internal errors, so elog is sufficient.
     */
    elog_if (matviewRel->rd_rel->relhasrules == false ||
        matviewRel->rd_rules->numLocks < 1,
			 ERROR, "materialized view \"%s\" is missing rewrite information", RelationGetRelationName(matviewRel));
    
    elog_if (matviewRel->rd_rules->numLocks > 1, ERROR, "materialized view \"%s\" has too many rules", RelationGetRelationName(matviewRel));
    
    rule = matviewRel->rd_rules->rules[0];
    elog_if (rule->event != CMD_SELECT || !(rule->isInstead), ERROR, "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule", RelationGetRelationName(matviewRel));
    
    actions = rule->actions;
    elog_if (list_length(actions) != 1, ERROR, "the rule for materialized view \"%s\" is not a single action", RelationGetRelationName(matviewRel));
    
    query = linitial_node (Query, actions);
    
    heap_close (matviewRel, lockmode);

    elog_if (g_trace_parse_select_query, INFO, "%s: parse tree: %s", __func__, nodeToString (query));
	
	query = mv_rewrite_rewrite_mv_query (query);

    *parsed_mv_query = query;
}

struct mv_rewrite_xform_todo_list {
    ListOf (Index) *replacement_var;
    ListOf (Expr *) *replaced_expr;
	Index replacement_varno;
};

static void
mv_rewrite_add_xform_todo_list_entry (struct mv_rewrite_xform_todo_list *todo_list,
									  Index i,
									  Expr *replaced_node,
									  TargetEntry *replacement_te)
{
	elog_if (g_trace_transform_vars, INFO, "%s: will replace node %s: with Var (varattrno=%d)", __func__, nodeToString (replaced_node), i);

	todo_list->replacement_var = lappend_int(todo_list->replacement_var, (int) i);
    todo_list->replaced_expr = lappend(todo_list->replaced_expr, replaced_node);
}

static Node *
mv_rewrite_xform_todos_mutator (Node *node,
								struct mv_rewrite_xform_todo_list *todo_list)
{
    ListCell *lc1, *lc2;
    forboth(lc1, todo_list->replaced_expr, lc2, todo_list->replacement_var)
    {
        Expr *replaced_node = (Expr *) lfirst (lc1);
        Index i = (Index) lfirst_int (lc2);
        
        if (replaced_node == (Expr *) node)
        {
            
            Var *v = makeNode (Var);
			
            v->varno = v->varnoold = todo_list->replacement_varno;
			
            v->varattno = v->varoattno = (int) i;
            v->varcollid = exprCollation ((Node *) replaced_node);
            v->varlevelsup = 0;
			v->vartype = exprType ((Node *) replaced_node);
            v->vartypmod = -1;
            v->location = -1;
            
			elog_if (g_trace_transform_vars, INFO, "%s: transforming node: %s into: %s", __func__, nodeToString (replaced_node), nodeToString (v));
            
            return (Node *) v;
        }
    }
    return expression_tree_mutator (node, mv_rewrite_xform_todos_mutator, todo_list);
}

static ListOf (Expr * or TargetEntry *) *
mv_rewrite_xform_todos (ListOf (Expr * or TargetEntry *) *tlist,
						struct mv_rewrite_xform_todo_list *todo_list)
{
    return (List *) expression_tree_mutator ((Node *) tlist, mv_rewrite_xform_todos_mutator, todo_list);
}

static bool
mv_rewrite_check_expr_targets_in_mv_tlist (PlannerInfo *root,
										   Query *parsed_mv_query,
										   Node *expr,
										   struct mv_rewrite_xform_todo_list *todo_list,
										   bool consider_sub_exprs,
										   bool trace);

struct mv_rewrite_expr_targets_equals_ctx_internal
{
	struct mv_rewrite_expr_targets_equals_ctx *com;
	bool attempt_communte_opex;
};

struct mv_rewrite_expr_targets_equals_ctx
{
    PlannerInfo *root;
    List *parsed_mv_query_rtable; // parsed_mv_query->rtable;
};

static bool
mv_rewrite_expr_targets_equals_walker (Node *query_expr,
									   Node *mv_expr,
									   struct mv_rewrite_expr_targets_equals_ctx_internal *ctx)
{
	if (query_expr != NULL &&  mv_expr != NULL)
	{
		if (nodeTag(query_expr) == T_Var && nodeTag(mv_expr) == T_Var)
		{
			// Check they reference the same query level...
			if (((Var *)query_expr)->varlevelsup != ((Var *)mv_expr)->varlevelsup)
				return false;
			
			// Check they have the same type...
			if (((Var *)query_expr)->vartype != ((Var *)mv_expr)->vartype)
				return false;

			// Check they reference the same typemod...
			if (((Var *)query_expr)->vartypmod != ((Var *)mv_expr)->vartypmod)
				return false;

			// Check they reference the same collation...
			if (((Var *)query_expr)->varcollid != ((Var *)mv_expr)->varcollid)
				return false;
			
			// Check neither is special...
			if (IS_SPECIAL_VARNO (((Var *)query_expr)->varno) ||
				IS_SPECIAL_VARNO (((Var *)mv_expr)->varno))
				return false;

			// Check they reference the same relation...
			if (planner_rt_fetch((int) ((Var *)query_expr)->varno, ctx->com->root)->relid !=
				list_nth_node (RangeTblEntry, ctx->com->parsed_mv_query_rtable, (int) ((Var *)mv_expr)->varno - 1)->relid)
				return false;

			// Check they are normal attribute references...
			if (((Var *)query_expr)->varattno <= 0 || ((Var *)mv_expr)->varattno <= 0)
				return false;

			Value *a_col_name = list_nth_node(Value,
											  planner_rt_fetch((int) ((Var *)query_expr)->varno, ctx->com->root)->eref->colnames,
											  (int) ((Var *)query_expr)->varattno - 1);
			
			Value *b_col_name = list_nth_node(Value,
											  list_nth_node (RangeTblEntry, ctx->com->parsed_mv_query_rtable, (int) ((Var *)mv_expr)->varno - 1)->eref->colnames,
											  (int) ((Var *)mv_expr)->varattno - 1);

			if (0 != strcmp (a_col_name->val.str, b_col_name->val.str))
				return false;
			
			return true;
		}
		else if (nodeTag(query_expr) == T_OpExpr && nodeTag(mv_expr) == T_OpExpr && ctx->attempt_communte_opex)
		{
			// We will attempt both to compare the expression commuted both ways ourselves, so
			// indicate to subordinate code that it shouldn't do so...
			ctx->attempt_communte_opex = false;
			
			//elog (INFO, "%s: comparing MV expression: %s", __func__, nodeToString (mv_expr));
			//elog (INFO, "%s: with planned query expression: %s", __func__, nodeToString (query_expr));

			// First, try it the natural way...
			if (equal_tree_walker (query_expr, mv_expr, mv_rewrite_expr_targets_equals_walker, ctx))
			{
				//elog (INFO, "%s: match.", __func__);

				return true;
			}
			else
			{
				if (!OidIsValid (get_commutator (((OpExpr *) query_expr)->opno)))
				{
					//elog (INFO, "%s: no commuted operator found.", __func__);
					
					return false;
				}
				
				OpExpr my_query_expr = *((OpExpr *) query_expr);
				my_query_expr.args = list_copy (((OpExpr *) query_expr)->args);
				
				// Commute the expression...
				CommuteOpExpr (&my_query_expr);
				
				//elog (INFO, "%s: trying with commuted version: %s", __func__, nodeToString (&my_query_expr));

				// Try the commuted way...
				bool result = equal_tree_walker ((Node *) &my_query_expr, mv_expr, mv_rewrite_expr_targets_equals_walker, ctx);
				
				//elog (INFO, "%s: %s.", __func__, result ? "match" : "no match");

				return result;
			}
		}
	}
	
	struct mv_rewrite_expr_targets_equals_ctx_internal inner_ctx = {
		ctx->com,
		true // re-enable commuting again
	};
	
    bool result = equal_tree_walker(query_expr, mv_expr, mv_rewrite_expr_targets_equals_walker, &inner_ctx);

    return result;
}

static bool
mv_rewrite_expr_targets_equal (Node *query_expr,
							   Node *mv_expr,
							   struct mv_rewrite_expr_targets_equals_ctx *ctx)
{
	struct mv_rewrite_expr_targets_equals_ctx_internal int_ctx = {
		ctx, true
	};
	return mv_rewrite_expr_targets_equals_walker (query_expr, mv_expr, &int_ctx);
}

struct mv_rewrite_expr_targets_in_mv_tlist_ctx
{
    ListOf (TargetEntry *) *tList;
    bool trace; // enable trace logging
    bool match_found;
    bool did_search_anything;
	bool consider_sub_exprs;
    struct mv_rewrite_xform_todo_list *todo_list;
    struct mv_rewrite_expr_targets_equals_ctx *col_names;
};

static bool
mv_rewrite_expr_targets_in_mv_tlist_walker (Node *node, struct mv_rewrite_expr_targets_in_mv_tlist_ctx *ctx)
{
    bool done = false;
    bool local_match_found = false;
    
    elog_if (ctx->trace, INFO, "%s: >>>>", __func__);
    
    ctx->did_search_anything = true;
    
    if (!ctx->match_found)
    {
        elog_if (ctx->trace, INFO, "%s: previous expr didn't match; no point continuing...", __func__);

        return false;
    }
    
    elog_if (ctx->trace, INFO, "%s: checking expr: %s", __func__, nodeToString (node));
    
    if (!done)
    {
        if (node == NULL)
        {
            done = true;
            
            elog_if (ctx->trace, INFO, "%s: NULL node", __func__);
        }
    }
    
    if (!done)
    {
        if (IsA (node, Const))
        {
            local_match_found = true;
            done = true;
            
            elog_if (ctx->trace, INFO, "%s: constants are always acceptable", __func__);
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
            
            elog_if (ctx->trace, INFO, "%s: against expr: %s", __func__, nodeToString (te->expr));
            
            local_match_found = mv_rewrite_expr_targets_equal ((Node *) node, (Node *) te->expr, ctx->col_names);
            
            if (local_match_found)
            {
                elog_if (ctx->trace, INFO, "%s: matched!", __func__);
				
                mv_rewrite_add_xform_todo_list_entry (ctx->todo_list, i, (Expr *) node, te);
                
                done = true;
                
                break;
            }
            i++;
        }
    }
	
	if (!done)
	{
		if (!ctx->consider_sub_exprs)
		{
			elog_if (ctx->trace, INFO, "%s: no match yet, but will not consider sub-expressions", __func__);

			local_match_found = false;
			
			done = true;
		}
	}

	if (!done)
    {
        // Copy current context
        struct mv_rewrite_expr_targets_in_mv_tlist_ctx sub_ctx = *ctx;
        
        sub_ctx.match_found = true; // walker will set to false if not matched
        sub_ctx.did_search_anything = false; // indicates whether match_found is valid
        
        elog_if (ctx->trace, INFO, "%s: walking sub-expressions...", __func__);
        
        // Look at the sub-expressions inside the Node. All must either match
        // exactly, or derive from something that does.
        expression_tree_walker (node, mv_rewrite_expr_targets_in_mv_tlist_walker, &sub_ctx);
        
        if (!sub_ctx.did_search_anything) {
            elog_if (ctx->trace, INFO, "%s: (no sub-expressions walked)", __func__);
        }
        
        elog_if (ctx->trace, INFO, "%s: done walking sub-expressions.", __func__);
        
        local_match_found = sub_ctx.did_search_anything && sub_ctx.match_found;
        
        done = true;
    }
    
    if (done)
    {
        elog_if (ctx->trace, INFO, "%s: done; local match found: %d", __func__, local_match_found);
        
        // We got a match: record it, but we can't abort the walk because
        // there may be adjacent arguments. Unfortunately, we can't directly
        // signal to go no deepper, but continue with the adjacent walk.
        ctx->match_found = ctx->match_found && local_match_found;
        
        elog_if (ctx->trace, INFO, "%s: done; resulting match indicator: %d", __func__, ctx->match_found);
    }
    
    elog_if (ctx->trace, INFO, "%s: <<<<", __func__);
    
    // Always continue to search adjacent expressions, even if we didn't dive down.
    return false;
}

/**
 * mv_rewrite_check_expr_targets_in_mv_tlist
 *
 * Check the given expression (expr, in the planned query, root) is
 * supported by one of the entries in parsed_mv_query's target list.
 *
 * For those expressions that are accepted, then add an entry for the expr
 * to the todo_list (entry is added as a pointer) so that we can fix it up
 * later to point at the parsed_mv_query tlist entry directly. The point is
 * that a whole Expr, or sub-Expr thereof, might be supported by a target
 * in the MV query. So we will need to replace that Expr (or sub-Expr) node
 * with a Var that references the relevant tlist entry.
 *
 * Set trace to enable trace-level logging.
 *
 * Set consider_sub_exprs to allow a match only of a sub-Expr: this is
 * appropriate when searching for matches for SELECT expressions (since
 * the outer elements of the Expr tree can still be used, even though only
 * the sub-Expr is directly provided by the MV); however it is not
 * appropriate when searching for matching GROUP BY clauses since the
 * whole Expr tree must match exactly.
 */
static bool
mv_rewrite_check_expr_targets_in_mv_tlist (PlannerInfo *root,
										   Query *parsed_mv_query,
										   Node *expr,
										   struct mv_rewrite_xform_todo_list *todo_list,
										   bool consider_sub_exprs,
										   bool trace)
{
    if (expr == NULL)
        return true;
    
    struct mv_rewrite_expr_targets_equals_ctx col_names = {
        root, parsed_mv_query->rtable
    };
    struct mv_rewrite_expr_targets_in_mv_tlist_ctx ctx = {
        .tList = parsed_mv_query->targetList,
		.match_found = true, // walker will set to false if not matched
		.trace = trace,
		.todo_list = todo_list,
		.col_names = &col_names,
		.consider_sub_exprs = consider_sub_exprs
    };
	
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

static void
mv_rewrite_extract_order_clauses (PlannerInfo *root,
								  RelationKind stage,
								  RelOptInfo *ordered_rel,
								  List *selected_tlist,
								  List **additional_sort_clauses)
{
	ListCell *query_lc;
	
	if (stage < UPPERREL_ORDERED)
		return;

	elog_if (g_trace_order_clause_source_check, INFO, "ordered_rel: %s", nodeToString(ordered_rel));
	elog_if (g_trace_order_clause_source_check, INFO, "selected_tlist: %s", nodeToString(selected_tlist));

	foreach (query_lc, root->sort_pathkeys)
	{
		PathKey *query_pk = lfirst (query_lc);
		
		elog_if (g_trace_order_clause_source_check, INFO, "pathkey: %s", nodeToString(query_pk));
		
		EquivalenceClass *query_ec = query_pk->pk_eclass;
		
		TargetEntry *query_tle = NULL;
		
		ListCell *emc;
		foreach (emc, query_ec->ec_members)
		{
			EquivalenceMember *em = lfirst_node (EquivalenceMember, emc);
			Expr *em_expr = em->em_expr;
			
			query_tle = tlist_member (em_expr, selected_tlist);
			
			if (query_tle != NULL)
				break;
		}
		
		if (query_tle == NULL)
			elog (ERROR, "could not find EM expression for sort pathkey");

		elog_if (g_trace_order_clause_source_check, INFO, "expr: %s", nodeToString(query_tle));
		
		Oid restype = exprType ((Node *) query_tle->expr);
		Oid sortop;
		Oid eqop;
		bool hashable;
		
		switch (query_pk->pk_strategy)
		{
			case BTLessStrategyNumber /* ASC */:
				get_sort_group_operators (restype, true, true, false, &sortop, &eqop, NULL, &hashable);
				break;
			case BTGreaterStrategyNumber /* DESC */:
				get_sort_group_operators (restype, false, true, true, NULL, &eqop, &sortop, &hashable);
				break;
			default:
				elog (ERROR, "sort strategy not determined");
				return;
		}
		
		SortGroupClause *sortcl = makeNode (SortGroupClause);
		
		sortcl->tleSortGroupRef = assignSortGroupRef (query_tle, selected_tlist);
		
		sortcl->eqop = eqop;
		sortcl->sortop = sortop;
		sortcl->hashable = hashable;
		sortcl->nulls_first = query_pk->pk_nulls_first;
		
		*additional_sort_clauses = lappend (*additional_sort_clauses, sortcl);
	}

	elog_if (g_trace_order_clause_source_check, INFO, "additional_sort_clauses: %s", nodeToString(*additional_sort_clauses));
}

static bool
mv_rewrite_check_order_clauses_for_mv (PlannerInfo *root,
									   RelationKind stage,
									   Query *parsed_mv_query,
									   RelOptInfo *ordered_rel)
{
	// If we have an MV including ORDER BY clauses, check we are rewriting for a
	// ordered_rel...
	if (list_length (parsed_mv_query->sortClause) == 0 &&
		(stage >= UPPERREL_ORDERED && list_length (root->sort_pathkeys) > 0))
	{
		elog_if (g_log_match_progress, INFO, "%s: looking to rewrite ORDER BY, but MV has no ORDER BY.", __func__);
		
		return false;
	}
	
	// If both MV and quey are not ORDER BY, then we are done here...
	if (ordered_rel == NULL)
		return true;
	
	// We can tolerate a more-ordered MV.
	if (stage >= UPPERREL_ORDERED &&
		list_length (root->sort_pathkeys) > list_length (parsed_mv_query->sortClause))
	{
		elog_if (g_log_match_progress, INFO, "%s: looking to rewrite ORDER BY, but MV has fewer sort keys.", __func__);
		
		return false;
	}
	
	struct mv_rewrite_expr_targets_equals_ctx comparison_context = {
		root, parsed_mv_query->rtable
	};
	
	// Check each ORDER BY clause (Expr) in turn...
	ListCell *query_lc, *mv_lc;
	forboth (query_lc, root->sort_pathkeys, mv_lc, parsed_mv_query->sortClause)
	{
		PathKey *query_pk = lfirst (query_lc);
		EquivalenceMember *query_pk_ecm = list_nth_node (EquivalenceMember, query_pk->pk_eclass->ec_members, 0);
		Expr *query_expr = query_pk_ecm->em_expr;
		
		SortGroupClause *mv_sgc = lfirst (mv_lc);
		Expr *mv_expr = get_sortgroupref_tle (mv_sgc->tleSortGroupRef, parsed_mv_query->targetList)->expr;
		
		elog_if (g_trace_order_clause_source_check, INFO, "%s: comparing query expression: %s", __func__, nodeToString (query_expr));
		elog_if (g_trace_order_clause_source_check, INFO, "%s: with MV expression: %s", __func__, nodeToString (mv_expr));
		
		// Check that the query Expr direclty matches its partner in the MV.
		if (!mv_rewrite_expr_targets_equal ((Node *) query_expr, (Node *) mv_expr, &comparison_context))
		{
			elog_if (g_log_match_progress, INFO, "%s: ORDER BY clause (%s) not found in query", __func__,
					 mv_rewrite_deparse_expression (parsed_mv_query->rtable, mv_expr));
			
			return false;
		}
		
		// Check the NULLs precedence, and sort direction.

		/* Find the operator in pg_amop --- failure shouldn't happen */
		Oid			opfamily, opcintype;
		int16		mv_sgc_strategy;
		if (!get_ordering_op_properties (mv_sgc->sortop, &opfamily, &opcintype, &mv_sgc_strategy))
			elog(ERROR, "operator %u is not a valid ordering operator", mv_sgc->sortop);
		
		if (query_pk->pk_nulls_first != mv_sgc->nulls_first ||
			query_pk->pk_strategy != mv_sgc_strategy)
		{
			elog_if (g_log_match_progress, INFO, "%s: ORDER BY clause (%s) NULLS precedence or strategy not does not match", __func__,
					 mv_rewrite_deparse_expression (parsed_mv_query->rtable, mv_expr));
			
			return false;
		}
	}
	
	return true;
}

static bool
mv_rewrite_check_distinct_clauses_for_mv (PlannerInfo *root,
									   RelationKind stage,
									   Query *parsed_mv_query,
									   RelOptInfo *distinct_rel,
									   List *selected_tlist,
									   struct mv_rewrite_xform_todo_list *todo_list)
{
	// If we have an MV including DINSTINCT clauses, check we are rewriting for a
	// distinct_rel...
	if (list_length (parsed_mv_query->distinctClause) > 0 &&
		(stage < UPPERREL_DISTINCT || list_length (root->parse->distinctClause) == 0))
	{
		elog_if (g_log_match_progress, INFO, "%s: looking to rewrite without DISTINCT, but MV is DISTINCT.", __func__);
		
		return false;
	}
	if (list_length (parsed_mv_query->distinctClause) == 0 &&
		(stage >= UPPERREL_DISTINCT && list_length (root->parse->distinctClause) > 0))
	{
		elog_if (g_log_match_progress, INFO, "%s: looking to rewrite DISTINCT, but MV has no DISTINCT.", __func__);

		return false;
	}
	
	// If both MV and quey are not DISTINCT, then we are done here...
	if (distinct_rel == NULL)
		return true;
	
	// TODO: in the above checks, we might in future:
	//   (a) tolerate an adequately sorted MV, then add an explicit Unique plan step around it
	//      - Maybe the planner will do this for us anyway...
	//   (b) tolerate instead of DISTINCT, a set of matching GROUP BY expressions
	
	if (list_length (root->parse->distinctClause) != list_length (parsed_mv_query->distinctClause))
	{
		elog_if (g_log_match_progress, INFO, "%s: DISTINCT list for MV and query differ in length.", __func__);
		
		return false;
	}
	
	struct mv_rewrite_expr_targets_equals_ctx comparison_context = {
		root, parsed_mv_query->rtable
	};
	
	// Check each DISTINCT clause (Expr) in turn...
	ListCell *query_lc, *mv_lc;
	forboth (query_lc, root->parse->distinctClause, mv_lc, parsed_mv_query->distinctClause)
	{
		SortGroupClause *query_sgc = lfirst (query_lc);
		Expr *query_expr = get_sortgroupref_tle (query_sgc->tleSortGroupRef, selected_tlist)->expr;
		
		SortGroupClause *mv_sgc = lfirst (mv_lc);
		Expr *mv_expr = get_sortgroupref_tle (mv_sgc->tleSortGroupRef, parsed_mv_query->targetList)->expr;
		
		elog_if (g_trace_distinct_clause_source_check, INFO, "%s: comparing query expression: %s", __func__, nodeToString (query_expr));
		elog_if (g_trace_distinct_clause_source_check, INFO, "%s: with MV expression: %s", __func__, nodeToString (mv_expr));
		
		// Check that the query Expr direclty matches its partner in the MV.
		if (!mv_rewrite_expr_targets_equal ((Node *) query_expr, (Node *) mv_expr, &comparison_context))
		{
			elog_if (g_log_match_progress, INFO, "%s: DISTINCT clause (%s) not found in query", __func__,
					 mv_rewrite_deparse_expression (parsed_mv_query->rtable, mv_expr));
			
			return false;
		}
	}
	
	return true;
}

static bool
mv_rewrite_check_group_clauses_for_mv (PlannerInfo *root,
								 RelationKind stage,
                                 Query *parsed_mv_query,
                                 RelOptInfo *grouped_rel,
                                 List *selected_tlist,
                                 struct mv_rewrite_xform_todo_list *todo_list,
								 const unsigned int log_indent)
{
	elog_if (g_trace_match_progress, INFO, "%*schecking for GROUP BY clause match...", log_indent, log_prefix);

	// 1. If the MV has no GROUP BY clause, then, either:
	//		a. we aren't at a stage where the output_rel needs one
	//			--> so, return OK
	//		b. we are at a stage when we need one, and:
	//			1. the GROUP BY lists match in length;
	//			2. and in content.
	// 2. However, if the MV /does/ have a GROUP BY clause, then, either:
	//		a. (Same as 1.b.), or;
	//		b. we aren't at a stage that needs one
	//			--> so return NO_MATCH
	
	const unsigned int inner_log_indent = log_indent + indent;
	
	if (list_length (parsed_mv_query->groupClause) == 0) // 1.
	{
		if (stage < UPPERREL_GROUP_AGG) // 1.a.
			return true;
		// Else check 1.b in code below.
	}
	else // 2.
	{
		if (stage < UPPERREL_GROUP_AGG) // 2.b.
		{
			elog_if (g_log_match_progress, INFO, "%*slooking to rewrite without GROUP BY, but MV is GROUP BY.", inner_log_indent, log_prefix);

			return false;
		}
		// Else check 2.a (== 1.b) in code below.
	}
	
	// 2.a.
	if (root->parse->groupClause != NIL && parsed_mv_query->groupClause == NIL)
	{
		elog_if (g_log_match_progress, INFO, "%*slooking to rewrite GROUP BY, but MV has no GROUP BY.", inner_log_indent, log_prefix);
		
		return false;
	}
	else if (root->parse->groupClause == NIL && parsed_mv_query->groupClause != NIL)
	{
		elog_if (g_log_match_progress, INFO, "%*slooking to rewrite without GROUP BY, but MV is GROUP BY.", inner_log_indent, log_prefix);
		
		return false;
	}
	else if (list_length (root->parse->groupClause) != list_length (parsed_mv_query->groupClause))
	{
		elog_if (g_log_match_progress, INFO, "%*slooking to rewrite GROUP BY but clause list for MV and query differ in length.", inner_log_indent, log_prefix);
		
		return false;
	}
	
	struct mv_rewrite_expr_targets_equals_ctx comparison_context = {
		root, parsed_mv_query->rtable
	};
	
	const unsigned int inner2_log_indent = inner_log_indent + indent;
	
    // 2.b.
	ListCell *query_lc, *mv_lc;
    forboth (query_lc, root->parse->groupClause, mv_lc, parsed_mv_query->groupClause)
    {
        SortGroupClause *query_sgc = lfirst (query_lc);
        Expr *query_expr = get_sortgroupref_tle (query_sgc->tleSortGroupRef, selected_tlist)->expr;
        
		SortGroupClause *mv_sgc = lfirst (mv_lc);
		Expr *mv_expr = get_sortgroupref_tle (mv_sgc->tleSortGroupRef, parsed_mv_query->targetList)->expr;
		
		elog_if (g_trace_group_clause_source_check, INFO, "%*scomparing query expression: %s", inner2_log_indent, log_prefix, nodeToString (query_expr));
		elog_if (g_trace_group_clause_source_check, INFO, "%*swith MV expression: %s", inner2_log_indent, log_prefix, nodeToString (mv_expr));

		// Check that the query Expr direclty matches its partner in the MV.
		if (!mv_rewrite_expr_targets_equal ((Node *) query_expr, (Node *) mv_expr, &comparison_context))
        {
            elog_if (g_log_match_progress, INFO, "%*sGROUP BY clause (%s) not found in query", inner2_log_indent, log_prefix,
					 mv_rewrite_deparse_expression (parsed_mv_query->rtable, mv_expr));
			
            return false;
        }
    }

    return true;
}

static ListOf (Expr *) *
mv_rewrite_list_append_and_flatten_bool_exprs (ListOf (Expr *) *list,
											   Node *node)
{
	if (IsA (node, List))
	{
		ListOf (Expr *) *flat_list = NIL;
		ListCell *lc;
		foreach (lc, (List *) node)
		{
			Node *expr = lfirst (lc);
			
			flat_list = mv_rewrite_list_append_and_flatten_bool_exprs (flat_list, expr);
		}
		return list_concat (list, flat_list);
	}
	else if (IsA (node, RestrictInfo))
	{
		return mv_rewrite_list_append_and_flatten_bool_exprs (list, (Node *) ((RestrictInfo *) node)->clause);
	}
	else if (IsA (node, BoolExpr))
	{
		if (((BoolExpr *) node)->boolop == AND_EXPR)
		{
			// Presumet there's nested AND_EXPRs with long clause lists.
			return mv_rewrite_list_append_and_flatten_bool_exprs (list, (Node *) ((BoolExpr *) node)->args);
		}
	}
	
	// Otherwise (we assume some other kind of Expr)...
	return lappend (list, node);
}

static bool
mv_rewrite_join_node_is_valid_for_plan_recurse (ParseState *pstate,
								  PlannerInfo *root,
								  Node *node,
			  					  Query *parsed_mv_query,
								  Relids join_relids,
								  List **mv_oids_involved,
								  Relids *query_relids_involved,
								  RelOptInfo **query_found_rel,
								  ListOf (RestrictInfo *) **query_clauses,
								  ListOf (RestrictInfo *) **collated_mv_quals,
								  unsigned int log_indent);

/**
 * Check for the presence of every join clause from the MV in the query plan.
 * If a clause is missing, then it means the MV cannot substitute for the query.
 * All 'found' clauses are removed from the query_clauses list.
 *
 * It is equally possible the query plan contained more clauses than the MV. Those
 * additional clauses are left in query_clauses.
 *
 * Note: this is destructive to the mv_join_quals list.
 */
static bool
mv_rewrite_join_clauses_are_valid (struct mv_rewrite_expr_targets_equals_ctx *comparison_context,
					ListOf (RestrictInfo * or Expr *) *mv_join_quals,
					ListOf (Expr *) **query_clauses,
					const unsigned int log_indent)
{
	elog_if (g_debug_join_clause_check, INFO, "%*schecking FROM/JOIN clauses from MV are present in the query plan...", log_indent, log_prefix);

	const unsigned int inner_log_indent = log_indent + indent;
	
	ListCell *lc;
	foreach (lc, mv_join_quals)
	{
		Expr *mv_jq = lfirst (lc);
		
		elog_if (g_debug_join_clause_check, INFO, "%*schecking FROM/JOIN expression: %s", inner_log_indent, log_prefix, mv_rewrite_deparse_expression (comparison_context->parsed_mv_query_rtable, mv_jq));
		
		const unsigned int inner2_log_indent = inner_log_indent + indent;
		
		bool found = false;
		Expr *found_query_expr = NULL;
		
		ListCell *q_lc;
		foreach (q_lc, *query_clauses)
		{
			Expr *query_expr = lfirst (q_lc);
			
			elog_if (g_trace_join_clause_check, INFO, "%*sagainst expression: %s", inner2_log_indent, log_prefix,
					 mv_rewrite_deparse_expression (comparison_context->root->parse->rtable, query_expr));
			
			if (mv_rewrite_expr_targets_equal ((Node *) query_expr, (Node *) mv_jq, comparison_context))
			{
				elog_if (g_debug_join_clause_check, INFO, "%*smatch found.", inner2_log_indent, log_prefix);
				
				found = true;
				found_query_expr = query_expr;
				break;
			}
		}
		
		if (found)
		{
			*query_clauses = list_delete_ptr (*query_clauses, found_query_expr);
		}
		else
		{
			elog_if (g_log_match_progress, INFO, "%*sMV expression (%s) not found in clauses in the query.", inner2_log_indent, log_prefix,
					 mv_rewrite_deparse_expression (comparison_context->parsed_mv_query_rtable, mv_jq));
			
			return false;
		}
	}
	
	return true;
}

/**
 * Check the FROM/JOIN clauses of the plan. The rel supplied (join_rel) must contain joins rels
 * and join clauses that match those in the plan.
 *
 * We don't check WHERE clauses in this routine, and so any non-FROM/JOIN cluases we find may be
 * returned in the list of additional_where_clauses.
 */
static bool
mv_rewrite_from_join_clauses_are_valid_for_mv (PlannerInfo *root,
											   Query *parsed_mv_query,
											   RelOptInfo *join_rel,
											   ListOf (Expr *) **additional_where_clauses,
											   const unsigned int log_indent)
{
	elog_if (g_debug_join_clause_check, INFO, "%*schecking MV's join tree is valid for the query plan...", log_indent, log_prefix);
	
	const unsigned int inner_log_indent = log_indent + indent;

	elog_if (g_trace_join_clause_check, INFO, "%*sjoin_rel: %s", inner_log_indent, log_prefix, nodeToString (join_rel));

	// Attempt to find a legal join in the plan for each of the joins in the MV's jointree.
	
	ListOf (Oid) *mv_oids_involved = NIL;
	Relids query_relids_involved = NULL;
	
	Relids join_relids = IS_UPPER_REL (join_rel) ? root->all_baserels : join_rel->relids;
	
	elog_if (g_trace_join_clause_check, INFO, "%*sjoin_relids: %s", inner_log_indent, log_prefix, bmsToString (join_relids));
	
	// As we walk the join tree, build up a list of quals. We will check each qual
	// for presence in the query later.
	ListOf (Expr *) *collated_query_quals = NIL;
	ListOf (RestrictInfo *) *collated_mv_quals = NIL;

	if (!mv_rewrite_join_node_is_valid_for_plan_recurse (NULL, root, (Node *) parsed_mv_query->jointree, parsed_mv_query, join_relids, &mv_oids_involved, &query_relids_involved, NULL, &collated_query_quals, &collated_mv_quals, inner_log_indent))
		return false;

	struct mv_rewrite_expr_targets_equals_ctx comparison_context = {
		root, parsed_mv_query->rtable
	};
	
	// Check each MV join clauses is present in the query plan.
	if (!mv_rewrite_join_clauses_are_valid (&comparison_context, collated_mv_quals, &collated_query_quals, inner_log_indent))
		return false;
	
	// Any query plan clauses not found will have to be folded as quals against the rewritten query.
	*additional_where_clauses = collated_query_quals;
	
	// The join should now have accounted for every relation in the. We check this now in case,
	// during the join search, we missed some relations that were joined in the plan, but not joined
	// by the MV.
	//
	// We have to make this comparison by OIDs, rather than Relids, which means we can't use
	// bms_equal().
	List *query_oids = NIL;
	ListOf (RelOptInfo *) *seen_ris = NIL;
	if (!mv_rewrite_find_oids_for_relids_recurse (root, join_relids, &query_oids, &seen_ris))
		return false;
	
	ListCell *lc;
	foreach (lc, mv_oids_involved)
	{
		Oid mv_oid = lfirst_oid (lc);
		
		query_oids = list_delete_oid (query_oids, mv_oid);
	}

	if (list_length (query_oids) != 0)
	{
		elog_if (g_log_match_progress, INFO, "%*sthe query does not involve all relations joined by the MV", inner_log_indent, log_prefix);

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
		// ...same for inh...
		my_a.inh = my_b.inh = false;

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

enum mv_rewrite_check_exprs_supported_action {
	StopOnFailReturnFalse,
	DeleteEntryButContinue
};
static bool
mv_rewrite_check_nodes_supported_by_mv_tlist (PlannerInfo *root,
											  Query *parsed_mv_query,
											  const char *clause_type,
											  ListOf (Expr *) **clause_list,
											  struct mv_rewrite_xform_todo_list *todo_list,
											  enum mv_rewrite_check_exprs_supported_action action,
											  bool log_progress,
											  bool trace_progress,
											  const unsigned int log_indent)
{
	elog_if (trace_progress, INFO, "%*schecking expressions are supported by MV SELECT list...", log_indent, log_prefix);

	const unsigned int inner_log_indent = log_indent + indent;
	
	ListCell   *lc;
	foreach (lc, *clause_list)
	{
		Node *node = lfirst (lc);
		
		elog_if (trace_progress, INFO, "%*schecking expression: %s", inner_log_indent, log_prefix, mv_rewrite_deparse_expression (root->parse->rtable,
																																  IsA (node, TargetEntry) ? ((TargetEntry *) node)->expr : (Expr *) node));

		const unsigned int inner2_log_indent = inner_log_indent + indent;

		// FIXME: we should check that none of the Expr's Vars references a levelsup > 0
		
		if (!mv_rewrite_check_expr_targets_in_mv_tlist (root, parsed_mv_query, node, todo_list, true, trace_progress))
		{
			elog_if (log_progress, INFO, "%*sclause (%s) can not be supported by MV SELECT list", inner2_log_indent, log_prefix,
					 mv_rewrite_deparse_expression (root->parse->rtable,
													IsA (node, TargetEntry) ? ((TargetEntry *) node)->expr : (Expr *) node));
			
			if (action == StopOnFailReturnFalse)
				return false;
			else if (action == DeleteEntryButContinue)
				*clause_list = list_delete (*clause_list, node);
		}
	}
	
	return true;
}

ListOf (Expr *) *
mv_rewrite_get_pushed_down_exprs (PlannerInfo *root,
								  RelOptInfo *rel,
								  Index relid,
								  const unsigned int log_indent)
{
	ListOf (Expr *) *out_ris = NIL;

	ListOf (RestrictInfo *) *rel_ris = NIL;

	if (IS_SIMPLE_REL (rel))
		rel_ris = rel->baserestrictinfo;
	else if (IS_JOIN_REL (rel))
		rel_ris = rel->joininfo;
	
	const unsigned inner_log_indent = log_indent + indent;
	
	ListCell *lc;
	foreach (lc, rel_ris)
	{
		RestrictInfo *rel_ri = lfirst_node (RestrictInfo, lc);
		
		if (rel_ri->is_pushed_down)
		{
			elog_if (g_trace_pushed_down_clauses_collation, INFO, "%*sfound pushed down clause: %s", inner_log_indent, log_prefix, mv_rewrite_deparse_expression (root->parse->rtable, rel_ri->clause));

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
			ListOf (Expr *) *subris = mv_rewrite_get_pushed_down_exprs (rel->subroot, subrel, (Index) i, inner_log_indent);

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
		elog (INFO, "%*scollated clause list: %s", inner_log_indent, log_prefix, cl->data);
	}

	// Done with this query level: thin out the list, and include only those
	// RI clauses that can be pulled back up. We do this by matching each
	// calsue against the returning subquery's target list.
	
	struct mv_rewrite_xform_todo_list xform_todo_list = {
		.replaced_expr = NIL,
		.replacement_var = NIL,
		.replacement_varno = relid
	};
	
	(void) mv_rewrite_check_nodes_supported_by_mv_tlist (subroot, subroot->parse, "pushed down", &sub_ris, &xform_todo_list, DeleteEntryButContinue, g_trace_pushed_down_clauses_collation, false, inner_log_indent);
	
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
		elog (INFO, "%*sresulting clause list: %s", inner_log_indent, log_prefix, cl->data);
	}

done:
	return out_ris;
}

static void
mv_rewrite_explain_rte (Query *query, Node *node, StringInfo out)
{
	if (IsA (node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				mv_rewrite_append_relname_for_oid (rte->relid, out);
				break;
			case RTE_SUBQUERY:
			{
				appendStringInfoString (out, "(subquery involving: ");
				ListCell *lc;
				bool first = true;
				foreach (lc, rte->subquery->rtable)
				{
					RangeTblEntry *rte = lfirst (lc);
					if (!first)
						appendStringInfoString (out, ", ");
					mv_rewrite_explain_rte (rte->subquery, (Node *) rte, out);
					first = false;
				}
				appendStringInfoString (out, ")");
				break;
			}
			case RTE_JOIN:
			{
				appendStringInfoString (out, "(join)"); // FIXME: of what?
				break;
			}
			case RTE_CTE:
				appendStringInfo (out, "CTE: %s", rte->ctename);
				break;
				
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_VALUES:
			case RTE_NAMEDTUPLESTORE:
			default:
				appendStringInfo (out, "(other: type %d)", rte->rtekind);
				break;
		}
	}
	else if (IsA (node, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) node;
		RangeTblEntry *mv_rte = rt_fetch (rtr->rtindex, query->rtable);
		mv_rewrite_explain_rte (query, (Node *) mv_rte, out);
	}
	else if (IsA (node, JoinExpr))
	{
		JoinExpr *je = castNode (JoinExpr, node);
		appendStringInfoString (out, "(");
		mv_rewrite_explain_rte (query, je->larg, out);
		appendStringInfo (out, " JOIN (type=%d) ", je->jointype);
		mv_rewrite_explain_rte (query, je->rarg, out);
		appendStringInfoString (out, ")");
	}
	else if (IsA (node, FromExpr))
	{
		FromExpr *fe = (FromExpr *) node;
		ListCell *lc;
		bool first = true;
		foreach (lc, fe->fromlist)
		{
			Node *child = lfirst (lc);
			if (!first)
				appendStringInfoString (out, ", ");
			mv_rewrite_explain_rte (query, (Node *) child, out);
			first = false;
		}
	}
	else
	{
		elog (WARNING, "%s: unknown node type: %d", __func__, node->type);
	}
}

static void
mv_rewrite_find_oids_for_subquery (ParseState *pstate, Query *query, ListOf (Oid) **oids_involved)
{
	if (pstate == NULL)
		pstate = make_parsestate (NULL);
	pstate->p_ctenamespace = query->cteList;

	ListCell *lc;
	foreach (lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst (lc);
		
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				*oids_involved = list_append_unique_oid (*oids_involved, rte->relid);
				break;
			case RTE_SUBQUERY:
			{
				mv_rewrite_find_oids_for_subquery (make_parsestate (pstate), rte->subquery, oids_involved);
				break;
			}
			case RTE_JOIN:
				break;
			case RTE_CTE:
			{
				// Avoid recursing infinitely into RECURISVE CTEs
				if (rte->self_reference)
					break;
				CommonTableExpr *cte = GetCTEForRTE (pstate, rte, 0);
				for (unsigned int i = 0; i < rte->ctelevelsup; i++)
					pstate = pstate->parentParseState;
				mv_rewrite_find_oids_for_subquery (make_parsestate (pstate), (Query *) cte->ctequery, oids_involved);
				break;
			}
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_VALUES:
			case RTE_NAMEDTUPLESTORE:
			default:
				elog (ERROR, "RTE kind (%d) not handled.", rte->rtekind);
				break;
		}
	}
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
mv_rewrite_join_node_is_valid_for_plan_recurse (ParseState *pstate,
								  PlannerInfo *root,
								  Node *node,
								  Query *parsed_mv_query,
								  Relids join_relids,
								  ListOf (Oid) **mv_oids_involved,
								  Relids *query_relids_involved,
								  RelOptInfo **query_found_rel,
								  ListOf (Epxr *) **collated_query_quals,
								  ListOf (RestrictInfo *) **collated_mv_quals,
								  unsigned int log_indent)
{
	if (pstate == NULL)
	{
		pstate = make_parsestate (NULL);
		pstate->p_ctenamespace = parsed_mv_query->cteList;
	}

	if (g_debug_join_clause_check)
	{
		StringInfo mv_query_join_str = makeStringInfo();
		mv_rewrite_explain_rte (parsed_mv_query, node, mv_query_join_str);
		elog_if (g_debug_join_clause_check, INFO, "%*schecking MV FROM/JOIN sub-tree (%s) is valid for the query plan...", log_indent, log_prefix, mv_query_join_str->data);
	}
	log_indent += indent;

	if (IsA (node, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) node;
		RangeTblEntry *mv_rte = rt_fetch (rtr->rtindex, parsed_mv_query->rtable);
		
		// Find the OID it references.
		Oid mv_oid = mv_rte->relid;
		
		// Locate the same OID the query plan.
		for (int i = bms_next_member (join_relids, -1);
			 i >= 0;
			 i = bms_next_member (join_relids, i))
		{
			RangeTblEntry *plan_rte = planner_rt_fetch (i, root);
			
			elog_if (g_trace_join_clause_check, INFO, "%splan RTE: %s", log_prefix, nodeToString (plan_rte));
			
			if (mv_rte->rtekind == plan_rte->rtekind && plan_rte->rtekind == RTE_RELATION)
			{
				Oid join_oid = plan_rte->relid;

				// If found, keep a record so we know what joins to what later.
				if (mv_oid == join_oid)
				{
					if (list_member_oid (*mv_oids_involved, mv_oid))
					{
						elog_if (g_log_match_progress, INFO, "%*sMV with multiple mentions of same OID not supported.", log_indent, log_prefix);
						
						return false;
					}
					
					*mv_oids_involved = list_append_unique_oid (*mv_oids_involved, mv_oid);
					*query_relids_involved = bms_add_member (*query_relids_involved, i);

					elog_if (g_trace_join_clause_check, INFO, "%*smatch found: MV rtindex: %d; MV Oid: %d; matches plan relid: %d", log_indent, log_prefix, rtr->rtindex, (int) mv_oid, i);

					RelOptInfo *rel = find_base_rel (root, i);
					if (rel->baserestrictinfo != NULL)
					{
						ListCell *lc;
						foreach (lc, rel->baserestrictinfo)
						{
							RestrictInfo *ri = lfirst (lc);

							*collated_query_quals = list_append_unique_ptr (*collated_query_quals, ri->clause);
						}
					}
					
					elog_if (g_trace_join_clause_check, INFO, "%*srel: %s", log_indent, log_prefix, nodeToString (rel));

					if (query_found_rel != NULL)
						*query_found_rel = rel;
					
					elog_if (g_debug_join_clause_check, INFO, "%*smatch found.", log_indent, log_prefix);

					return true;
				}
			}
			else if (mv_rte->rtekind == plan_rte->rtekind && plan_rte->rtekind == RTE_SUBQUERY)
			{
				if (equal_tree_walker (mv_rte, plan_rte, mv_rewrite_rte_equals_walker, NULL))
				{
					// Recurse down into the MV query to find the OIDs involved...
					mv_rewrite_find_oids_for_subquery (make_parsestate (pstate), mv_rte->subquery, mv_oids_involved);
					
					*query_relids_involved = bms_add_member (*query_relids_involved, i);
					
					elog_if (g_trace_join_clause_check, INFO, "%*smatch found: MV rtindex: %d; MV Oid: %d; matches plan relid: %d", log_indent, log_prefix, rtr->rtindex, (int) mv_oid, i);
					
					RelOptInfo *rel = find_base_rel (root, i);
					
					elog_if (g_trace_join_clause_check, INFO, "%*srel: %s", log_indent, log_prefix, nodeToString (rel));

					ListOf (Expr *) *pushed_quals = mv_rewrite_get_pushed_down_exprs (root, rel, (Index) i, log_indent);
					*collated_query_quals = list_concat_unique_ptr (*collated_query_quals, pushed_quals);

					if (query_found_rel != NULL)
						*query_found_rel = rel;

					elog_if (g_debug_join_clause_check, INFO, "%*smatch found.", log_indent, log_prefix);

					return true;
				}
			}
			else if (mv_rte->rtekind == plan_rte->rtekind && plan_rte->rtekind == RTE_CTE)
			{
				Index plan_ctendx;
				PlannerInfo *plan_cteroot;
				
				CommonTableExpr *plan_cte = mv_rewrite_get_cte_for_plan_rte (root, plan_rte, &plan_cteroot, &plan_ctendx);
				
				// We must be cautious: we are crawling widely over planner data
				// structures, but the plan may not yet be complete.
				if (plan_cte == NULL)
				{
					elog_if (g_debug_join_clause_check, INFO, "%*sCTE not found in plan.", log_indent, log_prefix);
					continue;
				}
				
				PlannerInfo *plan_cte_subroot = list_nth (plan_cteroot->glob->subroots, (int) plan_ctendx);
				if (plan_cte_subroot == NULL)
				{
					elog_if (g_debug_join_clause_check, INFO, "%*ssubroot for CTE not found.", log_indent, log_prefix);
					continue;
				}
				
				CommonTableExpr *mv_cte = GetCTEForRTE (pstate, mv_rte, 0);
				for (unsigned int i = 0; i < mv_rte->ctelevelsup; i++)
					pstate = pstate->parentParseState;
				
				if (equal_tree_walker (mv_cte->ctequery, plan_cte_subroot->parse, mv_rewrite_rte_equals_walker, NULL))
				{
					// Rrecurse into the MV subquery, and find all the OIDs involved in order that we can check it later.
					mv_rewrite_find_oids_for_subquery (make_parsestate (pstate), (Query *) mv_cte->ctequery, mv_oids_involved);

					elog_if (g_debug_join_clause_check, INFO, "%*smatch found.", log_indent, log_prefix);

					return true;
				}
				else
				{
					elog_if (g_debug_join_clause_check, INFO, "%*sCTE definitions don't smatch.", log_indent, log_prefix);

					continue;
				}
			}
		}

		// Otherwise, since we can't find a match, that means the MV doesn't match the query.

		elog_if (g_log_match_progress, INFO, "%*sno match found in plan for MV rtindex: %d; MV Oid: %d", log_indent, log_prefix, rtr->rtindex, (int) mv_oid);
		
		return false;
	}
	else if (IsA (node, JoinExpr))
	{
		JoinExpr *mv_je = castNode (JoinExpr, node);
		
		Node *mv_larg = mv_je->larg;
		Node *mv_rarg = mv_je->rarg;
		
		Relids larg_query_relids = NULL;
		
		// Find the RelOptInfos in the plan that represents larg and rarg
		RelOptInfo *lrel, *rrel;

		// Check the left side of the join is legal according to the plan.
		if (!mv_rewrite_join_node_is_valid_for_plan_recurse (pstate, root, mv_larg, parsed_mv_query, join_relids, mv_oids_involved, &larg_query_relids, &lrel, collated_query_quals, collated_mv_quals, log_indent))
		{
			elog_if (g_log_match_progress, INFO, "%*sleft side of join is not valid for plan", log_indent, log_prefix);

			return false;
		}

		Relids rarg_query_relids = NULL;
		
		// Check the right side of the join is legal according to the plan.
		if (!mv_rewrite_join_node_is_valid_for_plan_recurse (pstate, root, mv_rarg, parsed_mv_query, join_relids, mv_oids_involved, &rarg_query_relids, &rrel, collated_query_quals, collated_mv_quals, log_indent))
		{
			elog_if (g_log_match_progress, INFO, "%*sright side of join is not valid for plan", log_indent, log_prefix);
			
			return false;
		}
		
		elog_if (g_trace_join_clause_check, INFO, "%*schecking if join between %s and %s is legal...", log_indent, log_prefix, bmsToString (larg_query_relids), bmsToString (rarg_query_relids));

		Relids joinrelids = bms_union (larg_query_relids, rarg_query_relids);
		
		// FIXME: Check the two lists don't overlap!
		
		// Give the two legal sides of the join, check it is itself legal according to the plan.
		bool reversed;
		SpecialJoinInfo *sjinfo;
		
		if (!join_is_legal (root, lrel, rrel, joinrelids, &sjinfo, &reversed))
		{
			elog_if (g_log_match_progress, INFO, "%*sMV contains a join not matched in query.", log_indent, log_prefix);
			
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
	
		if (legal_jt != mv_je->jointype)
		{
			elog_if (g_log_match_progress, INFO, "%*sMV contains a join of type not matched in query.", log_indent, log_prefix);
				
			return false;
		}
		
		RelOptInfo *joinrel = find_join_rel (root, joinrelids);
		
		// Get hold of the JOIN clauses that relate to this join in the query.
		ListCell *lc;
		foreach (lc, joinrel->joininfo)
		{
			RestrictInfo *ri = lfirst (lc);
			
			*collated_query_quals = list_append_unique_ptr (*collated_query_quals, ri->clause);
		}
		// The list in joinrel doesn't include the implied ECs, nor those in the joined
		// rel's joininfo lists, so add them too.
		foreach (lc, build_joinrel_restrictlist (root, joinrel, rrel, lrel))
		{
			RestrictInfo *ri = lfirst (lc);
			
			*collated_query_quals = list_append_unique_ptr (*collated_query_quals, ri->clause);
		}

		// Similarly record the quals from the MV jointree.
		if (mv_je->quals != NULL)
		{
			*collated_mv_quals = mv_rewrite_list_append_and_flatten_bool_exprs (*collated_mv_quals, mv_je->quals);
		}
		
		elog_if (g_trace_join_clause_check, INFO, "%*sjoin for relids found: %s", log_indent, log_prefix, bmsToString (*query_relids_involved));
		
		*query_relids_involved = bms_union (*query_relids_involved, joinrelids);
		
		if (query_found_rel != NULL)
			*query_found_rel = joinrel;

		elog_if (g_debug_join_clause_check, INFO, "%*smatch found.", log_indent, log_prefix);

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
				*collated_mv_quals = mv_rewrite_list_append_and_flatten_bool_exprs (*collated_mv_quals, fe->quals);

			return mv_rewrite_join_node_is_valid_for_plan_recurse (pstate, root, list_nth_node (Node, fe->fromlist, 0), parsed_mv_query, join_relids, mv_oids_involved, query_relids_involved, NULL, collated_query_quals, collated_mv_quals, log_indent);
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
	
					if (!first)
						je->larg = je->rarg; // prior rarg becomes left
					
					je->rarg = child; // current child becomes right
					
					if (!first)
					{
						if (!mv_rewrite_join_node_is_valid_for_plan_recurse (pstate, root, (Node *) je, parsed_mv_query, join_relids, mv_oids_involved, &outrelids, NULL, collated_query_quals, collated_mv_quals, log_indent))
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
			{
				ListCell *lc;
				foreach (lc, outerrel->joininfo)
				{
					RestrictInfo *ri = lfirst (lc);
					
					*collated_query_quals = list_append_unique_ptr (*collated_query_quals, ri->clause);
				}
			}

			*query_relids_involved = bms_union (*query_relids_involved, outrelids);

			if (query_found_rel != NULL)
				*query_found_rel = outerrel;
		}
		
		// Record the quals from the MV jointree.
		if (fe->quals != NULL)
		{
			*collated_mv_quals = mv_rewrite_list_append_and_flatten_bool_exprs (*collated_mv_quals, fe->quals);
		}
		
		elog_if (g_trace_join_clause_check, INFO, "%*sjoin for relids found: %s", log_indent, log_prefix, bmsToString (*query_relids_involved));
		
		elog_if (g_debug_join_clause_check, INFO, "%*smatch found.", log_indent, log_prefix);
		
		return true;
	}
	else
		elog (ERROR, "unrecognized node type: %d", (int) nodeTag (node));
	
	return false;
}

static Query *
mv_rewrite_build_mv_scan_query (Oid matviewOid,
								const char *mv_name,
								ListOf (Value *) *colnames,
								ListOf (Expr *) *mv_scan_quals,
								ListOf (SortGroupClause *) *mv_sgc_list,
								ListOf (TargetEntry *) *transformed_tlist)
{
	Query *mv_scan_query = makeNode (Query);

	mv_scan_query->commandType = 1;
	mv_scan_query->targetList = transformed_tlist;

	RangeTblEntry *mvsqrte = makeNode (RangeTblEntry);
	mvsqrte->relid = matviewOid;
	mvsqrte->inh = true;
	mvsqrte->inFromCl = true;
	mvsqrte->requiredPerms = ACL_SELECT;
	mvsqrte->selectedCols = NULL;
	ListCell *lc;
	foreach (lc, mv_scan_query->targetList)
	{
		TargetEntry *te = lfirst (lc);
		pull_varattnos ((Node *) te->expr, 1, &mvsqrte->selectedCols);
	}
	mvsqrte->relkind = RELKIND_MATVIEW;
	mvsqrte->eref = makeNode (Alias);
	mvsqrte->eref->aliasname = (char *) mv_name;
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
	
	mv_scan_query->sortClause = mv_sgc_list;
	
	return mv_scan_query;
}

static bool
mv_rewrite_evaluate_mv_for_rewrite (PlannerInfo *root,
									RelationKind stage,
									RelOptInfo *input_rel,
									RelOptInfo *output_rel,
									List *selected_tlist,
									const char *mv_name,
									Query **alternative_query,
									const unsigned int log_indent)
{
	bool ok = false;
	
    struct mv_rewrite_xform_todo_list transform_todo_list = {
		.replaced_expr = NIL,
		.replacement_var = NIL,
		.replacement_varno = 1
	};
    
	Oid matviewOid;
	
	Query *parsed_mv_query;
	mv_rewrite_get_mv_query (mv_name, &parsed_mv_query, &matviewOid);
	
	// TODO: strategy:
	// check ORDER BY but onyl if stage >= OB
	// etc.
	// Obviously always check WHERE/FROM/JOIN
	
	// The query obtained which represents the MV is now locked.
	
    // 1a. Check the GROUP BY clause: it must match exactly.
	if (!mv_rewrite_check_group_clauses_for_mv (root, stage, parsed_mv_query, output_rel, selected_tlist, &transform_todo_list, log_indent))
    	goto done;
	
	// 1b. Check the DISTINCT clause: it must match exactly.
	if (!mv_rewrite_check_distinct_clauses_for_mv (root, stage, parsed_mv_query, output_rel, selected_tlist, &transform_todo_list))
		goto done;
	
	ListOf (Expr *) *additional_sort_clauses = NIL;
    ListOf (Expr *) *additional_where_clauses = NIL;
	
	// 1c. We don't have to consider UPPERREL_WINDOW specially: either the aggregate
	//     expressions match, or they don't.
    
	// 1d. Check the ORDER BY clause: it must match exactly.
	if (!mv_rewrite_check_order_clauses_for_mv (root, stage, parsed_mv_query, output_rel))
	{
		// 1e. But we have an alternative strategy for ORDER BY: if it is missing or incorrect,
		//     we can wrap an ORDER BY around the MV scan query. This stratey gives the
		//     planner benefit of any INDEXes that adorn the MV.
		mv_rewrite_extract_order_clauses (root, stage, output_rel, selected_tlist, &additional_sort_clauses);
	}
	
	// 2. Check the FROM and JOIN-related WHERE clauses: they must match exactly; any non-
	//    JOIN-related WHERE clauses are returned and validated in step 4.
    if (!mv_rewrite_from_join_clauses_are_valid_for_mv (root, parsed_mv_query, output_rel, &additional_where_clauses, log_indent))
        goto done;
    
    // 3. Append the HAVING clauses in the additional WHERE clause list. (They will actually be
    //    evaluated as part of the WHERE clause procesing.)
	if (output_rel != NULL)
		additional_where_clauses = list_concat (additional_where_clauses, list_copy ((List *) root->parse->havingQual));
	
    // 4. Check the additional WHERE clauses list, and any WHERE clauses not found in the FROM/JOIN list
    if (!mv_rewrite_check_nodes_supported_by_mv_tlist (root, parsed_mv_query, "WHERE", &additional_where_clauses, &transform_todo_list, StopOnFailReturnFalse, g_log_match_progress, g_trace_where_clause_source_check, log_indent))
        goto done;
    
    // 5. Check the SELECT clauses: they must be a subset of the MV's tList
    if (!mv_rewrite_check_nodes_supported_by_mv_tlist (root, parsed_mv_query, "SELECT", &selected_tlist, &transform_todo_list, StopOnFailReturnFalse, g_log_match_progress, g_trace_select_clause_source_check, log_indent))
        goto done;

    // 6. Transform Exprs that were found to match Exprs in the MV into Vars that instead reference MV tList entries
    ListOf (TargetEntry *) *transformed_tlist = mv_rewrite_xform_todos (selected_tlist, &transform_todo_list);
	ListOf (Expr *) *quals = mv_rewrite_xform_todos (additional_where_clauses, &transform_todo_list);
	
    // 7. Build the alternative query.
	*alternative_query = mv_rewrite_build_mv_scan_query (matviewOid, mv_name,
														 mv_rewrite_get_query_tlist_colnames (parsed_mv_query),
														 quals, additional_sort_clauses, transformed_tlist);
	
    ok = true;
	
done:
	if (parsed_mv_query != NULL && !ok)
		ReleaseRewriteLocks (parsed_mv_query, true, false);
	
	return ok;
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
	if (list_length (involved_rel_names) == 0)
	{
		elog (WARNING, "%s: no relations involved in query; no rewrite possible.", __func__);
		
		return false;
	}
	
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
	ListCell *lc2;
	foreach (lc2, involved_rel_names)
	{
		const char *involved_rel_name = lfirst (lc2);
		
		// Don't rewrite anything involving this because otherwise we'll recurse
		// indefinitely.
		if (strcmp (involved_rel_name, "mv_rewrite.pgx_rewritable_matviews") == 0)
			return false;
		
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
								RelOptInfo *rel,
								List *selected_tlist,
								PathTarget *pathtarget,
								const char *mv_name)
{
	/* plan_params should not be in use in current query level */
	elog_if (root->plan_params != NIL, ERROR, "%s: plan_params != NIL", __func__);
	
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
	cp->path.pathtarget = pathtarget;
	cp->path.parent = rel;
	
	cp->path.param_info = NULL;
	cp->path.parallel_aware = subplan->parallel_aware;
	cp->path.parallel_safe = subplan->parallel_safe;
	
	// Work out the cost of the competing (unrewritten) plan...
	StringInfo competing_cost = makeStringInfo();

	ListCell *lc;
	foreach (lc, rel->pathlist)
	{
		Path *p = lfirst_node (Path, lc);
		
		if (competing_cost->len > 0)
			appendStringInfo (competing_cost, "; ");
		
		appendStringInfo (competing_cost, "cost=%.2f..%.2f rows=%.0f width=%d",
						  p->startup_cost, p->total_cost, p->rows, p->pathtarget->width);
	}
	
	elog_if (g_trace_match_progress, INFO, "%s: rewritten cost: cost=%.2f..%.2f rows=%.0f width=%d", __func__,
			 	cp->path.startup_cost, cp->path.total_cost, cp->path.rows, cp->path.pathtarget->width);
	elog_if (g_trace_match_progress, INFO, "%s: competing costs: %s", __func__, competing_cost->data);
	
	StringInfo alt_query_text = makeStringInfo();
	appendStringInfo (alt_query_text, "scan of %s%s%s",
			 mv_name,
			 alternative_query->jointree->quals != NULL ? " WHERE " : "",
			 alternative_query->jointree->quals != NULL ?
			 mv_rewrite_deparse_expression (alternative_query->rtable, (Expr *) alternative_query->jointree->quals)
			 : "");

	cp->custom_private = list_make5 (pstmt, selected_tlist, rel->relids,
									 makeString (alt_query_text->data), makeString (competing_cost->data));
	
	return (Path *) cp;
}

static void
mv_rewrite_add_rewritten_mv_paths (PlannerInfo *root,
								   RelationKind stage,
								   RelOptInfo *input_rel,
								   RelOptInfo *output_rel,
								   PathTarget *pathtarget,
								   const unsigned int log_indent)
{
	if (!g_rewrite_enabled)
		return;
	
    // Find the set of rels involved in the query being planned...
	ListOf (char *) *involved_rel_names = NIL;
	
	if (!mv_rewrite_get_involved_rel_names (root, stage, output_rel, &involved_rel_names))
		return;

	// Check first that all of those rels are enabled for query rewrite...
	if (!mv_rewrite_involved_rels_enabled_for_rewrite (involved_rel_names))
	{
		elog_if (g_log_match_progress, INFO, "%*sMV rewrite not enabled for one or more table in the query.", log_indent, log_prefix);

		return;
	}

	// See if there are any candidate MVs...
	ListOf (Value(String)) *mvs_name = mv_rewrite_find_related_mvs_for_rel (involved_rel_names);
	
	if (mvs_name == NULL ||
		list_length (mvs_name) == 0)
	{
		if (g_log_match_progress)
		{
			FmgrInfo	a2t_proc;
			fmgr_info (F_ARRAY_TO_TEXT, &a2t_proc);

			elog(INFO, "%*sno candidate MVs for query involving {%s}.", log_indent, log_prefix,
				 TextDatumGetCString (FunctionCall2Coll (&a2t_proc, InvalidOid,
														 PointerGetDatum (strlist_to_textarray (involved_rel_names)),
														 PointerGetDatum (cstring_to_text (",")))));
		}

		return;
	}
    
	ListOf (TargetEntry *) *selected_tlist = make_tlist_from_pathtarget (pathtarget);
	
    // Evaluate each MV in turn...
    ListCell   *nc;
    foreach (nc, mvs_name)
    {
		char *mv_name = strVal (lfirst(nc));
        
        elog_if (g_log_match_progress, INFO, "%*sevaluating MV: %s", log_indent, log_prefix, mv_name);
		
		const unsigned int inner_log_indent = log_indent + indent;

        Query *alternative_query;
		
		if (mv_rewrite_evaluate_mv_for_rewrite (root, stage, input_rel, output_rel, selected_tlist, mv_name, &alternative_query, inner_log_indent))
		{
			// 8. Finally, create and add the path.
			elog_if (g_log_match_progress, INFO, "%*screating and adding path for scan on: %s%s%s", inner_log_indent, log_prefix,
					 mv_name,
					 alternative_query->jointree->quals != NULL ? " WHERE " : "",
					 alternative_query->jointree->quals != NULL ?
					 mv_rewrite_deparse_expression (alternative_query->rtable, (Expr *) alternative_query->jointree->quals)
					 : "");
			
			// Add generated path into the appropriate rel.
			RelOptInfo *the_rel = output_rel != NULL ? output_rel : input_rel;
			
			// But if root has init_paths...
			//		1. Morph this root into Custom Append-like Subquery
//			PlannerInfo *current_root = makeNode (PlannerInfo);
//			*current_root = *root;
//
//			root->init_plans = NULL;
//			root->cte_plan_ids = NULL;
//			root->upper_rels = { 0 };
//			the_rel = fetch_upper_rel (root, UPPERREL_FINAL, NULL);
//
//			create_append_path (current_root, the_rel, fetch_upper_rel (current_root, UPPERREL_FINAL, NULL)->pathlist, NIL, current_root->query_pathkeys, NULL, 0, false, NIL, 1)
//			add_path (<#RelOptInfo *parent_rel#>, <#Path *new_path#>)
			
			//		2. Hang current root off it as alternate #1
			//				- Under CustomPath #1
			//				- Cost Path as normal ::== cost of copied root's UPPERREL_FINAL
			//				- I guess we have to adjust all depth references += 1
			// 		3. Hang our new path off it as alternate #2
			//				- Under CustomPath #2
			// 		4. Pick Path as normal — based on least cost
			add_path (the_rel,
					  mv_rewrite_create_mv_scan_path (root, alternative_query, the_rel, selected_tlist, pathtarget, mv_name));
		}
    }
}
