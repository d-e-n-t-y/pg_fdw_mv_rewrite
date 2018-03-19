/*-------------------------------------------------------------------------
 *
 * postgres_fdw.c
 *		  Foreign-data wrapper for remote PostgreSQL servers
 *
 * Portions Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/postgres_fdw/postgres_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postgres_fdw.h"
#include "deparse.h"
#include "equalswalker.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "nodes/nodes.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"
#include "tcop/tcopprot.h"

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
    /* SQL statement to execute remotely (as a String node) */
    FdwScanPrivateSelectSql,
    /* Integer list of attribute numbers retrieved by the SELECT */
    FdwScanPrivateRetrievedAttrs,
    /* Integer representing the desired fetch_size */
    FdwScanPrivateFetchSize,
    
    /*
     * String describing join i.e. names of relations being joined and types
     * of join, added when the scan is join
     */
    FdwScanPrivateRelations
};

/*
 * Execution state of a foreign scan using postgres_fdw.
 */
typedef struct PgFdwScanState
{
    Relation	rel;			/* relcache entry for the foreign table. NULL
                                 * for a foreign join scan. */
    TupleDesc	tupdesc;		/* tuple descriptor of scan */
    AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */
    
    /* extracted fdw_private data */
    char	   *query;			/* text of SELECT command */
    List	   *retrieved_attrs;	/* list of retrieved attribute numbers */
    
    /* for remote query execution */
    PGconn	   *conn;			/* connection for the scan */
    unsigned int cursor_number; /* quasi-unique ID for my cursor */
    bool		cursor_exists;	/* have we created the cursor? */
    int			numParams;		/* number of parameters passed to query */
    FmgrInfo   *param_flinfo;	/* output conversion functions for them */
    List	   *param_exprs;	/* executable expressions for param values */
    const char **param_values;	/* textual values of query parameters */
    
    /* for storing result tuples */
    HeapTuple  *tuples;			/* array of currently-retrieved tuples */
    int			num_tuples;		/* # of tuples in array */
    int			next_tuple;		/* index of next one to return */
    
    /* batch-level state, for optimizing rewinds and avoiding useless fetch */
    int			fetch_ct_2;		/* Min(# of fetches done, 2) */
    bool		eof_reached;	/* true if last fetch reached EOF */
    
    /* working memory contexts */
    MemoryContext batch_cxt;	/* context holding current batch of tuples */
    MemoryContext temp_cxt;		/* context for per-tuple temporary data */
    
    int			fetch_size;		/* number of tuples per fetch */
} PgFdwScanState;

/*
 * Workspace for analyzing a foreign table.
 */
typedef struct PgFdwAnalyzeState
{
    Relation	rel;			/* relcache entry for the foreign table */
    AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */
    List	   *retrieved_attrs;	/* attr numbers retrieved by query */
    
    /* collected sample rows */
    HeapTuple  *rows;			/* array of size targrows */
    int			targrows;		/* target # of sample rows */
    int			numrows;		/* # of sample rows collected */
    
    /* for random sampling */
    double		samplerows;		/* # of rows fetched */
    double		rowstoskip;		/* # of rows to skip before next sample */
    ReservoirStateData rstate;	/* state for reservoir sampling */
    
    /* working memory contexts */
    MemoryContext anl_cxt;		/* context for per-analyze lifespan data */
    MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} PgFdwAnalyzeState;

/*
 * Identify the attribute where data conversion fails.
 */
typedef struct ConversionLocation
{
    Relation	rel;			/* foreign table's relcache entry. */
    AttrNumber	cur_attno;		/* attribute number being processed, or 0 */
    
    /*
     * In case of foreign join push down, fdw_scan_tlist is used to identify
     * the Var node corresponding to the error location and
     * fsstate->ss.ps.state gives access to the RTEs of corresponding relation
     * to get the relation name and attribute name.
     */
    ForeignScanState *fsstate;
} ConversionLocation;

/* Callback argument for ec_member_matches_foreign */
typedef struct
{
    Expr	   *current;		/* current expr, or NULL if not yet found */
    List	   *already_used;	/* expressions already dealt with */
} ec_member_foreign_arg;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(pg_fdw_mv_rewrite_handler);

/*
 * FDW callback routines
 */
static void postgresGetForeignRelSize(PlannerInfo *root,
                                      RelOptInfo *baserel,
                                      Oid foreigntableid);
static void postgresGetForeignPaths(PlannerInfo *root,
                                    RelOptInfo *baserel,
                                    Oid foreigntableid);
static ForeignScan *postgresGetForeignPlan(PlannerInfo *root,
                                           RelOptInfo *baserel,
                                           Oid foreigntableid,
                                           ForeignPath *best_path,
                                           List *tlist,
                                           List *scan_clauses,
                                           Plan *outer_plan);
static void postgresBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *postgresIterateForeignScan(ForeignScanState *node);
static void postgresReScanForeignScan(ForeignScanState *node);
static void postgresEndForeignScan(ForeignScanState *node);
static void postgresExplainForeignScan(ForeignScanState *node,
                                       ExplainState *es);
static List *postgresImportForeignSchema(ImportForeignSchemaStmt *stmt,
                                         Oid serverOid);
static void postgresGetForeignJoinPaths(PlannerInfo *root,
                                        RelOptInfo *joinrel,
                                        RelOptInfo *outerrel,
                                        RelOptInfo *innerrel,
                                        JoinType jointype,
                                        JoinPathExtraData *extra);
static bool postgresRecheckForeignScan(ForeignScanState *node,
                                       TupleTableSlot *slot);
static void postgresGetForeignUpperPaths(PlannerInfo *root,
                                         UpperRelationKind stage,
                                         RelOptInfo *input_rel,
                                         RelOptInfo *output_rel);

/*
 * Helper functions
 */
static void estimate_path_cost_size(PlannerInfo *root,
                                    RelOptInfo *baserel,
                                    List *join_conds,
                                    List *pathkeys,
                                    double *p_rows, int *p_width,
                                    Cost *p_startup_cost, Cost *p_total_cost);
static void get_remote_estimate(const char *sql,
                                PGconn *conn,
                                double *rows,
                                int *width,
                                Cost *startup_cost,
                                Cost *total_cost);
static bool ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
                                      EquivalenceClass *ec, EquivalenceMember *em,
                                      void *arg);
static void create_cursor(ForeignScanState *node);
static void fetch_more_data(ForeignScanState *node);
static void close_cursor(PGconn *conn, unsigned int cursor_number);
static void prepare_query_params(PlanState *node,
                                 List *fdw_exprs,
                                 int numParams,
                                 FmgrInfo **param_flinfo,
                                 List **param_exprs,
                                 const char ***param_values);
static void process_query_params(ExprContext *econtext,
                                 FmgrInfo *param_flinfo,
                                 List *param_exprs,
                                 const char **param_values);
static HeapTuple make_tuple_from_result_row(PGresult *res,
                                            int row,
                                            Relation rel,
                                            AttInMetadata *attinmeta,
                                            List *retrieved_attrs,
                                            ForeignScanState *fsstate,
                                            MemoryContext temp_context);
static void conversion_error_callback(void *arg);
static bool foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
                            JoinType jointype, RelOptInfo *outerrel, RelOptInfo *innerrel,
                            JoinPathExtraData *extra);
static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel);
static List *get_useful_pathkeys_for_relation(PlannerInfo *root,
                                              RelOptInfo *rel);
static List *get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel);
static void add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
                                            Path *epq_path);
static void add_foreign_grouping_paths(PlannerInfo *root,
                                       RelOptInfo *input_rel,
                                       RelOptInfo *grouped_rel);
static void apply_server_options(PgFdwRelationInfo *fpinfo);
static void apply_table_options(PgFdwRelationInfo *fpinfo);
static void merge_fdw_options(PgFdwRelationInfo *fpinfo,
                              const PgFdwRelationInfo *fpinfo_o,
                              const PgFdwRelationInfo *fpinfo_i);


/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
pg_fdw_mv_rewrite_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *routine = makeNode(FdwRoutine);
    
    /* Functions for scanning foreign tables */
    routine->GetForeignRelSize = postgresGetForeignRelSize;
    routine->GetForeignPaths = postgresGetForeignPaths;
    routine->GetForeignPlan = postgresGetForeignPlan;
    routine->BeginForeignScan = postgresBeginForeignScan;
    routine->IterateForeignScan = postgresIterateForeignScan;
    routine->ReScanForeignScan = postgresReScanForeignScan;
    routine->EndForeignScan = postgresEndForeignScan;
    
    /* Functions for updating foreign tables */
    routine->AddForeignUpdateTargets = NULL;
    routine->PlanForeignModify = NULL;
    routine->BeginForeignModify = NULL;
    routine->ExecForeignInsert = NULL;
    routine->ExecForeignUpdate = NULL;
    routine->ExecForeignDelete = NULL;
    routine->EndForeignModify = NULL;
    routine->IsForeignRelUpdatable = NULL;
    routine->PlanDirectModify = NULL;
    routine->BeginDirectModify = NULL;
    routine->IterateDirectModify = NULL;
    routine->EndDirectModify = NULL;
    
    /* Function for EvalPlanQual rechecks */
    routine->RecheckForeignScan = postgresRecheckForeignScan;
    /* Support functions for EXPLAIN */
    routine->ExplainForeignScan = postgresExplainForeignScan;
    routine->ExplainForeignModify = NULL;
    routine->ExplainDirectModify = NULL;
    
    /* Support functions for ANALYZE */
    routine->AnalyzeForeignTable = NULL;
    
    /* Support functions for IMPORT FOREIGN SCHEMA */
    routine->ImportForeignSchema = postgresImportForeignSchema;
    
    /* Support functions for join push-down */
    routine->GetForeignJoinPaths = postgresGetForeignJoinPaths;
    
    /* Support functions for upper relation push-down */
    routine->GetForeignUpperPaths = postgresGetForeignUpperPaths;
    
    PG_RETURN_POINTER(routine);
}

/*
 * postgresGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void
postgresGetForeignRelSize(PlannerInfo *root,
                          RelOptInfo *baserel,
                          Oid foreigntableid)
{
    PgFdwRelationInfo *fpinfo;
    ListCell   *lc;
    RangeTblEntry *rte = planner_rt_fetch((int) baserel->relid, root);
    const char *namespace;
    const char *relname;
    const char *refname;
    
    elog(INFO, "%s (root=%p, baserel=%p, foreigntableid=%x)", __func__, root, baserel, foreigntableid);
    
    /*
     * We use PgFdwRelationInfo to pass various information to subsequent
     * functions.
     */
    fpinfo = (PgFdwRelationInfo *) palloc0(sizeof(PgFdwRelationInfo));
    baserel->fdw_private = (void *) fpinfo;
    
    /* Base foreign tables need to be pushed down always. */
    fpinfo->pushdown_safe = true;
    
    /* Look up foreign-table catalog info. */
    fpinfo->table = GetForeignTable(foreigntableid);
    fpinfo->server = GetForeignServer(fpinfo->table->serverid);
    
    /*
     * Extract user-settable option values.  Note that per-table setting of
     * use_remote_estimate overrides per-server setting.
     */
    fpinfo->use_remote_estimate = false;
    fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
    fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
    fpinfo->shippable_extensions = NIL;
    fpinfo->fetch_size = 100;
    
    apply_server_options(fpinfo);
    apply_table_options(fpinfo);
    
    /*
     * If the table or the server is configured to use remote estimates,
     * identify which user to do remote access as during planning.  This
     * should match what ExecCheckRTEPerms() does.  If we fail due to lack of
     * permissions, the query would have failed at runtime anyway.
     */
    if (fpinfo->use_remote_estimate)
    {
        Oid			userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
        
        fpinfo->user = GetUserMapping(userid, fpinfo->server->serverid);
    }
    else
        fpinfo->user = NULL;
    
    /*
     * Identify which baserestrictinfo clauses can be sent to the remote
     * server and which can't.
     */
    classifyConditions(root, baserel, baserel->baserestrictinfo,
                       &fpinfo->remote_conds, &fpinfo->local_conds);
    
    /*
     * Identify which attributes will need to be retrieved from the remote
     * server.  These include all attrs needed for joins or final output, plus
     * all attrs used in the local_conds.  (Note: if we end up using a
     * parameterized scan, it's possible that some of the join clauses will be
     * sent to the remote and thus we wouldn't really need to retrieve the
     * columns used in them.  Doesn't seem worth detecting that case though.)
     */
    fpinfo->attrs_used = NULL;
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
                   &fpinfo->attrs_used);
    foreach(lc, fpinfo->local_conds)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
        
        pull_varattnos((Node *) rinfo->clause, baserel->relid,
                       &fpinfo->attrs_used);
    }
    
    /*
     * Compute the selectivity and cost of the local_conds, so we don't have
     * to do it over again for each path.  The best we can do for these
     * conditions is to estimate selectivity on the basis of local statistics.
     */
    fpinfo->local_conds_sel = clauselist_selectivity(root,
                                                     fpinfo->local_conds,
                                                     (int) baserel->relid,
                                                     JOIN_INNER,
                                                     NULL);
    
    cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);
    
    /*
     * Set cached relation costs to some negative value, so that we can detect
     * when they are set to some sensible costs during one (usually the first)
     * of the calls to estimate_path_cost_size().
     */
    fpinfo->rel_startup_cost = -1;
    fpinfo->rel_total_cost = -1;
    
    /*
     * If the table or the server is configured to use remote estimates,
     * connect to the foreign server and execute EXPLAIN to estimate the
     * number of rows selected by the restriction clauses, as well as the
     * average row width.  Otherwise, estimate using whatever statistics we
     * have locally, in a way similar to ordinary tables.
     */
    if (fpinfo->use_remote_estimate)
    {
        /*
         * Get cost/size estimates with help of remote server.  Save the
         * values in fpinfo so we don't need to do it again to generate the
         * basic foreign path.
         */
        estimate_path_cost_size(root, baserel, NIL, NIL,
                                &fpinfo->rows, &fpinfo->width,
                                &fpinfo->startup_cost, &fpinfo->total_cost);
        
        /* Report estimated baserel size to planner. */
        baserel->rows = fpinfo->rows;
        baserel->reltarget->width = fpinfo->width;
    }
    else
    {
        /*
         * If the foreign table has never been ANALYZEd, it will have relpages
         * and reltuples equal to zero, which most likely has nothing to do
         * with reality.  We can't do a whole lot about that if we're not
         * allowed to consult the remote server, but we can use a hack similar
         * to plancat.c's treatment of empty relations: use a minimum size
         * estimate of 10 pages, and divide by the column-datatype-based width
         * estimate to get the corresponding number of tuples.
         */
        if (baserel->pages == 0 && baserel->tuples == 0)
        {
            baserel->pages = 10;
            baserel->tuples =
            (10 * BLCKSZ) / ((size_t) baserel->reltarget->width +
                             MAXALIGN(SizeofHeapTupleHeader));
        }
        
        /* Estimate baserel size as best we can with local statistics. */
        set_baserel_size_estimates(root, baserel);
        
        /* Fill in basically-bogus cost estimates for use later. */
        estimate_path_cost_size(root, baserel, NIL, NIL,
                                &fpinfo->rows, &fpinfo->width,
                                &fpinfo->startup_cost, &fpinfo->total_cost);
    }
    
    /*
     * Set the name of relation in fpinfo, while we are constructing it here.
     * It will be used to build the string describing the join relation in
     * EXPLAIN output. We can't know whether VERBOSE option is specified or
     * not, so always schema-qualify the foreign table name.
     */
    fpinfo->relation_name = makeStringInfo();
    namespace = get_namespace_name(get_rel_namespace(foreigntableid));
    relname = get_rel_name(foreigntableid);
    refname = rte->eref->aliasname;
    appendStringInfo(fpinfo->relation_name, "%s.%s",
                     quote_identifier(namespace),
                     quote_identifier(relname));
    if (*refname && strcmp(refname, relname) != 0)
        appendStringInfo(fpinfo->relation_name, " %s",
                         quote_identifier(rte->eref->aliasname));
    
    /* No outer and inner relations. */
    fpinfo->make_outerrel_subquery = false;
    fpinfo->make_innerrel_subquery = false;
    fpinfo->lower_subquery_rels = NULL;
    /* Set the relation index. */
    fpinfo->relation_index = (int) baserel->relid;
}

/*
 * get_useful_ecs_for_relation
 *		Determine which EquivalenceClasses might be involved in useful
 *		orderings of this relation.
 *
 * This function is in some respects a mirror image of the core function
 * pathkeys_useful_for_merging: for a regular table, we know what indexes
 * we have and want to test whether any of them are useful.  For a foreign
 * table, we don't know what indexes are present on the remote side but
 * want to speculate about which ones we'd like to use if they existed.
 *
 * This function returns a list of potentially-useful equivalence classes,
 * but it does not guarantee that an EquivalenceMember exists which contains
 * Vars only from the given relation.  For example, given ft1 JOIN t1 ON
 * ft1.x + t1.x = 0, this function will say that the equivalence class
 * containing ft1.x + t1.x is potentially useful.  Supposing ft1 is remote and
 * t1 is local (or on a different server), it will turn out that no useful
 * ORDER BY clause can be generated.  It's not our job to figure that out
 * here; we're only interested in identifying relevant ECs.
 */
static List *
get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
    List	   *useful_eclass_list = NIL;
    ListCell   *lc;
    Relids		relids;
    
    /*
     * First, consider whether any active EC is potentially useful for a merge
     * join against this relation.
     */
    if (rel->has_eclass_joins)
    {
        foreach(lc, root->eq_classes)
        {
            EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);
            
            if (eclass_useful_for_merging(root, cur_ec, rel))
                useful_eclass_list = lappend(useful_eclass_list, cur_ec);
        }
    }
    
    /*
     * Next, consider whether there are any non-EC derivable join clauses that
     * are merge-joinable.  If the joininfo list is empty, we can exit
     * quickly.
     */
    if (rel->joininfo == NIL)
        return useful_eclass_list;
    
    /* If this is a child rel, we must use the topmost parent rel to search. */
    if (IS_OTHER_REL(rel))
    {
        Assert(!bms_is_empty(rel->top_parent_relids));
        relids = rel->top_parent_relids;
    }
    else
        relids = rel->relids;
    
    /* Check each join clause in turn. */
    foreach(lc, rel->joininfo)
    {
        RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);
        
        /* Consider only mergejoinable clauses */
        if (restrictinfo->mergeopfamilies == NIL)
            continue;
        
        /* Make sure we've got canonical ECs. */
        update_mergeclause_eclasses(root, restrictinfo);
        
        /*
         * restrictinfo->mergeopfamilies != NIL is sufficient to guarantee
         * that left_ec and right_ec will be initialized, per comments in
         * distribute_qual_to_rels.
         *
         * We want to identify which side of this merge-joinable clause
         * contains columns from the relation produced by this RelOptInfo. We
         * test for overlap, not containment, because there could be extra
         * relations on either side.  For example, suppose we've got something
         * like ((A JOIN B ON A.x = B.x) JOIN C ON A.y = C.y) LEFT JOIN D ON
         * A.y = D.y.  The input rel might be the joinrel between A and B, and
         * we'll consider the join clause A.y = D.y. relids contains a
         * relation not involved in the join class (B) and the equivalence
         * class for the left-hand side of the clause contains a relation not
         * involved in the input rel (C).  Despite the fact that we have only
         * overlap and not containment in either direction, A.y is potentially
         * useful as a sort column.
         *
         * Note that it's even possible that relids overlaps neither side of
         * the join clause.  For example, consider A LEFT JOIN B ON A.x = B.x
         * AND A.x = 1.  The clause A.x = 1 will appear in B's joininfo list,
         * but overlaps neither side of B.  In that case, we just skip this
         * join clause, since it doesn't suggest a useful sort order for this
         * relation.
         */
        if (bms_overlap(relids, restrictinfo->right_ec->ec_relids))
            useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
                                                        restrictinfo->right_ec);
        else if (bms_overlap(relids, restrictinfo->left_ec->ec_relids))
            useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
                                                        restrictinfo->left_ec);
    }
    
    return useful_eclass_list;
}

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
    List	   *useful_pathkeys_list = NIL;
    List	   *useful_eclass_list;
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) rel->fdw_private;
    EquivalenceClass *query_ec = NULL;
    ListCell   *lc;
    
    /*
     * Pushing the query_pathkeys to the remote server is always worth
     * considering, because it might let us avoid a local sort.
     */
    if (root->query_pathkeys)
    {
        bool		query_pathkeys_ok = true;
        
        foreach(lc, root->query_pathkeys)
        {
            PathKey    *pathkey = (PathKey *) lfirst(lc);
            EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
            Expr	   *em_expr;
            
            /*
             * The planner and executor don't have any clever strategy for
             * taking data sorted by a prefix of the query's pathkeys and
             * getting it to be sorted by all of those pathkeys. We'll just
             * end up resorting the entire data set.  So, unless we can push
             * down all of the query pathkeys, forget it.
             *
             * is_foreign_expr would detect volatile expressions as well, but
             * checking ec_has_volatile here saves some cycles.
             */
            if (pathkey_ec->ec_has_volatile ||
                !(em_expr = find_em_expr_for_rel(pathkey_ec, rel)) ||
                !is_foreign_expr(root, rel, em_expr))
            {
                query_pathkeys_ok = false;
                break;
            }
        }
        
        if (query_pathkeys_ok)
            useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
    }
    
    /*
     * Even if we're not using remote estimates, having the remote side do the
     * sort generally won't be any worse than doing it locally, and it might
     * be much better if the remote side can generate data in the right order
     * without needing a sort at all.  However, what we're going to do next is
     * try to generate pathkeys that seem promising for possible merge joins,
     * and that's more speculative.  A wrong choice might hurt quite a bit, so
     * bail out if we can't use remote estimates.
     */
    if (!fpinfo->use_remote_estimate)
        return useful_pathkeys_list;
    
    /* Get the list of interesting EquivalenceClasses. */
    useful_eclass_list = get_useful_ecs_for_relation(root, rel);
    
    /* Extract unique EC for query, if any, so we don't consider it again. */
    if (list_length(root->query_pathkeys) == 1)
    {
        PathKey    *query_pathkey = linitial(root->query_pathkeys);
        
        query_ec = query_pathkey->pk_eclass;
    }
    
    /*
     * As a heuristic, the only pathkeys we consider here are those of length
     * one.  It's surely possible to consider more, but since each one we
     * choose to consider will generate a round-trip to the remote side, we
     * need to be a bit cautious here.  It would sure be nice to have a local
     * cache of information about remote index definitions...
     */
    foreach(lc, useful_eclass_list)
    {
        EquivalenceClass *cur_ec = lfirst(lc);
        Expr	   *em_expr;
        PathKey    *pathkey;
        
        /* If redundant with what we did above, skip it. */
        if (cur_ec == query_ec)
            continue;
        
        /* If no pushable expression for this rel, skip it. */
        em_expr = find_em_expr_for_rel(cur_ec, rel);
        if (em_expr == NULL || !is_foreign_expr(root, rel, em_expr))
            continue;
        
        /* Looks like we can generate a pathkey, so let's do it. */
        pathkey = make_canonical_pathkey(root, cur_ec,
                                         linitial_oid(cur_ec->ec_opfamilies),
                                         BTLessStrategyNumber,
                                         false);
        useful_pathkeys_list = lappend(useful_pathkeys_list,
                                       list_make1(pathkey));
    }
    
    return useful_pathkeys_list;
}

/*
 * postgresGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
static void
postgresGetForeignPaths(PlannerInfo *root,
                        RelOptInfo *baserel,
                        Oid foreigntableid)
{
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) baserel->fdw_private;
    ForeignPath *path;
    List	   *ppi_list;
    ListCell   *lc;
    
    //elog(INFO, "%s (root=%p, baserel=%p, foreigntableid=%x)", __func__, root, baserel, foreigntableid);
    
    /*
     * Create simplest ForeignScan path node and add it to baserel.  This path
     * corresponds to SeqScan path of regular tables (though depending on what
     * baserestrict conditions we were able to send to remote, there might
     * actually be an indexscan happening there).  We already did all the work
     * to estimate cost and size of this path.
     */
    path = create_foreignscan_path(root, baserel,
                                   NULL,	/* default pathtarget */
                                   fpinfo->rows,
                                   fpinfo->startup_cost,
                                   fpinfo->total_cost,
                                   NIL, /* no pathkeys */
                                   NULL,	/* no outer rel either */
                                   NULL,	/* no extra plan */
                                   NIL);	/* no fdw_private list */
    add_path(baserel, (Path *) path);
    
    /* Add paths with pathkeys */
    add_paths_with_pathkeys_for_rel(root, baserel, NULL);
    
    /*
     * If we're not using remote estimates, stop here.  We have no way to
     * estimate whether any join clauses would be worth sending across, so
     * don't bother building parameterized paths.
     */
    if (!fpinfo->use_remote_estimate)
        return;
    
    /*
     * Thumb through all join clauses for the rel to identify which outer
     * relations could supply one or more safe-to-send-to-remote join clauses.
     * We'll build a parameterized path for each such outer relation.
     *
     * It's convenient to manage this by representing each candidate outer
     * relation by the ParamPathInfo node for it.  We can then use the
     * ppi_clauses list in the ParamPathInfo node directly as a list of the
     * interesting join clauses for that rel.  This takes care of the
     * possibility that there are multiple safe join clauses for such a rel,
     * and also ensures that we account for unsafe join clauses that we'll
     * still have to enforce locally (since the parameterized-path machinery
     * insists that we handle all movable clauses).
     */
    ppi_list = NIL;
    foreach(lc, baserel->joininfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
        Relids		required_outer;
        ParamPathInfo *param_info;
        
        /* Check if clause can be moved to this rel */
        if (!join_clause_is_movable_to(rinfo, baserel))
            continue;
        
        /* See if it is safe to send to remote */
        if (!is_foreign_expr(root, baserel, rinfo->clause))
            continue;
        
        /* Calculate required outer rels for the resulting path */
        required_outer = bms_union(rinfo->clause_relids,
                                   baserel->lateral_relids);
        /* We do not want the foreign rel itself listed in required_outer */
        required_outer = bms_del_member(required_outer, (int) baserel->relid);
        
        /*
         * required_outer probably can't be empty here, but if it were, we
         * couldn't make a parameterized path.
         */
        if (bms_is_empty(required_outer))
            continue;
        
        /* Get the ParamPathInfo */
        param_info = get_baserel_parampathinfo(root, baserel,
                                               required_outer);
        Assert(param_info != NULL);
        
        /*
         * Add it to list unless we already have it.  Testing pointer equality
         * is OK since get_baserel_parampathinfo won't make duplicates.
         */
        ppi_list = list_append_unique_ptr(ppi_list, param_info);
    }
    
    /*
     * The above scan examined only "generic" join clauses, not those that
     * were absorbed into EquivalenceClauses.  See if we can make anything out
     * of EquivalenceClauses.
     */
    if (baserel->has_eclass_joins)
    {
        /*
         * We repeatedly scan the eclass list looking for column references
         * (or expressions) belonging to the foreign rel.  Each time we find
         * one, we generate a list of equivalence joinclauses for it, and then
         * see if any are safe to send to the remote.  Repeat till there are
         * no more candidate EC members.
         */
        ec_member_foreign_arg arg;
        
        arg.already_used = NIL;
        for (;;)
        {
            List	   *clauses;
            
            /* Make clauses, skipping any that join to lateral_referencers */
            arg.current = NULL;
            clauses = generate_implied_equalities_for_column(root,
                                                             baserel,
                                                             ec_member_matches_foreign,
                                                             (void *) &arg,
                                                             baserel->lateral_referencers);
            
            /* Done if there are no more expressions in the foreign rel */
            if (arg.current == NULL)
            {
                Assert(clauses == NIL);
                break;
            }
            
            /* Scan the extracted join clauses */
            foreach(lc, clauses)
            {
                RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
                Relids		required_outer;
                ParamPathInfo *param_info;
                
                /* Check if clause can be moved to this rel */
                if (!join_clause_is_movable_to(rinfo, baserel))
                    continue;
                
                /* See if it is safe to send to remote */
                if (!is_foreign_expr(root, baserel, rinfo->clause))
                    continue;
                
                /* Calculate required outer rels for the resulting path */
                required_outer = bms_union(rinfo->clause_relids,
                                           baserel->lateral_relids);
                required_outer = bms_del_member(required_outer, (int) baserel->relid);
                if (bms_is_empty(required_outer))
                    continue;
                
                /* Get the ParamPathInfo */
                param_info = get_baserel_parampathinfo(root, baserel,
                                                       required_outer);
                Assert(param_info != NULL);
                
                /* Add it to list unless we already have it */
                ppi_list = list_append_unique_ptr(ppi_list, param_info);
            }
            
            /* Try again, now ignoring the expression we found this time */
            arg.already_used = lappend(arg.already_used, arg.current);
        }
    }
    
    /*
     * Now build a path for each useful outer relation.
     */
    foreach(lc, ppi_list)
    {
        ParamPathInfo *param_info = (ParamPathInfo *) lfirst(lc);
        double		rows;
        int			width;
        Cost		startup_cost;
        Cost		total_cost;
        
        /* Get a cost estimate from the remote */
        estimate_path_cost_size(root, baserel,
                                param_info->ppi_clauses, NIL,
                                &rows, &width,
                                &startup_cost, &total_cost);
        
        /*
         * ppi_rows currently won't get looked at by anything, but still we
         * may as well ensure that it matches our idea of the rowcount.
         */
        param_info->ppi_rows = rows;
        
        /* Make the path */
        path = create_foreignscan_path(root, baserel,
                                       NULL,	/* default pathtarget */
                                       rows,
                                       startup_cost,
                                       total_cost,
                                       NIL, /* no pathkeys */
                                       param_info->ppi_req_outer,
                                       NULL,
                                       NIL);	/* no fdw_private list */
        add_path(baserel, (Path *) path);
    }
}

/*
 * postgresGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan *
postgresGetForeignPlan(PlannerInfo *root,
                       RelOptInfo *foreignrel,
                       Oid foreigntableid,
                       ForeignPath *best_path,
                       List *tlist,
                       List *scan_clauses,
                       Plan *outer_plan)
{
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;
    Index		scan_relid;
    List	   *fdw_private = best_path->fdw_private;
    List	   *remote_exprs = NIL;
    List	   *local_exprs = NIL;
    List	   *params_list = NIL;
    List	   *fdw_scan_tlist = NIL;
    List	   *fdw_recheck_quals = NIL;
    List	   *retrieved_attrs;
    StringInfoData sql;
    ListCell   *lc;
    
    //elog(INFO, "%s (root=%p, foreignrel=%p, foreigntableid=%x, best_path=%p, tlist=%p, scan_clauses=%p, outer_plan=%p", __func__, root, foreignrel, foreigntableid, best_path, tlist, scan_clauses, outer_plan);
    
    if (IS_SIMPLE_REL(foreignrel))
    {
        /*
         * For base relations, set scan_relid as the relid of the relation.
         */
        scan_relid = foreignrel->relid;
        
        /*
         * In a base-relation scan, we must apply the given scan_clauses.
         *
         * Separate the scan_clauses into those that can be executed remotely
         * and those that can't.  baserestrictinfo clauses that were
         * previously determined to be safe or unsafe by classifyConditions
         * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
         * else in the scan_clauses list will be a join clause, which we have
         * to check for remote-safety.
         *
         * Note: the join clauses we see here should be the exact same ones
         * previously examined by postgresGetForeignPaths.  Possibly it'd be
         * worth passing forward the classification work done then, rather
         * than repeating it here.
         *
         * This code must match "extract_actual_clauses(scan_clauses, false)"
         * except for the additional decision about remote versus local
         * execution.
         */
        foreach(lc, scan_clauses)
        {
            RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
            
            /* Ignore any pseudoconstants, they're dealt with elsewhere */
            if (rinfo->pseudoconstant)
                continue;
            
            if (list_member_ptr(fpinfo->remote_conds, rinfo))
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            else if (list_member_ptr(fpinfo->local_conds, rinfo))
                local_exprs = lappend(local_exprs, rinfo->clause);
            else if (is_foreign_expr(root, foreignrel, rinfo->clause))
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            else
                local_exprs = lappend(local_exprs, rinfo->clause);
        }
        
        /*
         * For a base-relation scan, we have to support EPQ recheck, which
         * should recheck all the remote quals.
         */
        fdw_recheck_quals = remote_exprs;
    }
    else
    {
        /*
         * Join relation or upper relation - set scan_relid to 0.
         */
        scan_relid = 0;
        
        /*
         * For a join rel, baserestrictinfo is NIL and we are not considering
         * parameterization right now, so there should be no scan_clauses for
         * a joinrel or an upper rel either.
         */
        Assert(!scan_clauses);
        
        /*
         * Instead we get the conditions to apply from the fdw_private
         * structure.
         */
        remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
        local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
        
        /*
         * We leave fdw_recheck_quals empty in this case, since we never need
         * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
         * recheck is handled elsewhere --- see postgresGetForeignJoinPaths().
         * If we're planning an upperrel (ie, remote grouping or aggregation)
         * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
         * allowed, and indeed we *can't* put the remote clauses into
         * fdw_recheck_quals because the unaggregated Vars won't be available
         * locally.
         */
        
        /* Build the list of columns to be fetched from the foreign server. */
        fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
        
        /*
         * Ensure that the outer plan produces a tuple whose descriptor
         * matches our scan tuple slot. This is safe because all scans and
         * joins support projection, so we never need to insert a Result node.
         * Also, remove the local conditions from outer plan's quals, lest
         * they will be evaluated twice, once by the local plan and once by
         * the scan.
         */
        if (outer_plan)
        {
            ListCell   *lc;
            
            /*
             * Right now, we only consider grouping and aggregation beyond
             * joins. Queries involving aggregates or grouping do not require
             * EPQ mechanism, hence should not have an outer plan here.
             */
            Assert(!IS_UPPER_REL(foreignrel));
            
            outer_plan->targetlist = fdw_scan_tlist;
            
            foreach(lc, local_exprs)
            {
                Join	   *join_plan = (Join *) outer_plan;
                Node	   *qual = lfirst(lc);
                
                outer_plan->qual = list_delete(outer_plan->qual, qual);
                
                /*
                 * For an inner join the local conditions of foreign scan plan
                 * can be part of the joinquals as well.
                 */
                if (join_plan->jointype == JOIN_INNER)
                    join_plan->joinqual = list_delete(join_plan->joinqual,
                                                      qual);
            }
        }
    }
    
    /*
     * Build the query string to be sent for execution, and identify
     * expressions to be sent as parameters.
     */
    initStringInfo(&sql);
    
    // If we have an already prepared alternative SQL, use that instead.
    if (!fdw_private)
    {
        deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
                                remote_exprs, best_path->path.pathkeys,
                                false, &retrieved_attrs, &params_list);
        /* Remember remote_exprs for possible use by postgresPlanDirectModify */
        fpinfo->final_remote_exprs = remote_exprs;
        
        /*
         * Build the fdw_private list that will be available to the executor.
         * Items in the list must match order in enum FdwScanPrivateIndex.
         */
        fdw_private = list_make3(makeString(sql.data),
                                 retrieved_attrs,
                                 makeInteger(fpinfo->fetch_size));
    }
    
    if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
        fdw_private = lappend(fdw_private,
                              makeString(fpinfo->relation_name->data));
    
    /*
     * Create the ForeignScan node for the given relation.
     *
     * Note that the remote parameter expressions are stored in the fdw_exprs
     * field of the finished plan node; we can't keep them in private state
     * because then they wouldn't be subject to later planner processing.
     */
    return make_foreignscan(tlist,
                            local_exprs,
                            scan_relid,
                            params_list,
                            fdw_private,
                            fdw_scan_tlist,
                            fdw_recheck_quals,
                            outer_plan);
}

/*
 * postgresBeginForeignScan
 *		Initiate an executor scan of a foreign PostgreSQL table.
 */
static void
postgresBeginForeignScan(ForeignScanState *node, int eflags)
{
    ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
    EState	   *estate = node->ss.ps.state;
    PgFdwScanState *fsstate;
    RangeTblEntry *rte;
    Oid			userid;
    ForeignTable *table;
    UserMapping *user;
    int			rtindex;
    int			numParams;
    
    //elog(INFO, "%s (node=%p, eflags=%x)", __func__, node, eflags);
    
    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;
    
    /*
     * We'll save private state in node->fdw_state.
     */
    fsstate = (PgFdwScanState *) palloc0(sizeof(PgFdwScanState));
    node->fdw_state = (void *) fsstate;
    
    /*
     * Identify which user to do the remote access as.  This should match what
     * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
     * lowest-numbered member RTE as a representative; we would get the same
     * result from any.
     */
    if (fsplan->scan.scanrelid > 0)
        rtindex = (int) fsplan->scan.scanrelid;
    else
        rtindex = bms_next_member(fsplan->fs_relids, -1);
    rte = rt_fetch(rtindex, estate->es_range_table);
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
    
    /* Get info about foreign table. */
    table = GetForeignTable(rte->relid);
    user = GetUserMapping(userid, table->serverid);
    
    /*
     * Get connection to the foreign server.  Connection manager will
     * establish new connection if necessary.
     */
    fsstate->conn = GetConnection(user, false);
    
    /* Assign a unique ID for my cursor */
    fsstate->cursor_number = GetCursorNumber(fsstate->conn);
    fsstate->cursor_exists = false;
    
    /* Get private info created by planner functions. */
    fsstate->query = strVal(list_nth(fsplan->fdw_private,
                                     FdwScanPrivateSelectSql));
    fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
                                                 FdwScanPrivateRetrievedAttrs);
    fsstate->fetch_size = (int) intVal(list_nth(fsplan->fdw_private,
                                                FdwScanPrivateFetchSize));
    
    /* Create contexts for batches of tuples and per-tuple temp workspace. */
    fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                               "postgres_fdw tuple data",
                                               ALLOCSET_DEFAULT_SIZES);
    fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                              "postgres_fdw temporary data",
                                              ALLOCSET_SMALL_SIZES);
    
    /*
     * Get info we'll need for converting data fetched from the foreign server
     * into local representation and error reporting during that process.
     */
    if (fsplan->scan.scanrelid > 0)
    {
        fsstate->rel = node->ss.ss_currentRelation;
        fsstate->tupdesc = RelationGetDescr(fsstate->rel);
    }
    else
    {
        fsstate->rel = NULL;
        fsstate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    }
    
    fsstate->attinmeta = TupleDescGetAttInMetadata(fsstate->tupdesc);
    
    /*
     * Prepare for processing of parameters used in remote query, if any.
     */
    numParams = list_length(fsplan->fdw_exprs);
    fsstate->numParams = numParams;
    if (numParams > 0)
        prepare_query_params((PlanState *) node,
                             fsplan->fdw_exprs,
                             numParams,
                             &fsstate->param_flinfo,
                             &fsstate->param_exprs,
                             &fsstate->param_values);
}

/*
 * postgresIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot *
postgresIterateForeignScan(ForeignScanState *node)
{
    PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    
    //elog(INFO, "%s (node=%p)", __func__, node);
    
    /*
     * If this is the first call after Begin or ReScan, we need to create the
     * cursor on the remote side.
     */
    if (!fsstate->cursor_exists)
        create_cursor(node);
    
    /*
     * Get some more tuples, if we've run out.
     */
    if (fsstate->next_tuple >= fsstate->num_tuples)
    {
        /* No point in another fetch if we already detected EOF, though. */
        if (!fsstate->eof_reached)
            fetch_more_data(node);
        /* If we didn't get any tuples, must be end of data. */
        if (fsstate->next_tuple >= fsstate->num_tuples)
            return ExecClearTuple(slot);
    }
    
    /*
     * Return the next tuple.
     */
    ExecStoreTuple(fsstate->tuples[fsstate->next_tuple++],
                   slot,
                   InvalidBuffer,
                   false);
    
    return slot;
}

/*
 * postgresReScanForeignScan
 *		Restart the scan.
 */
static void
postgresReScanForeignScan(ForeignScanState *node)
{
    PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;
    char		sql[64];
    PGresult   *res;
    
    //elog(INFO, "%s (node=%p", __func__, node);
    
    /* If we haven't created the cursor yet, nothing to do. */
    if (!fsstate->cursor_exists)
        return;
    
    /*
     * If any internal parameters affecting this node have changed, we'd
     * better destroy and recreate the cursor.  Otherwise, rewinding it should
     * be good enough.  If we've only fetched zero or one batch, we needn't
     * even rewind the cursor, just rescan what we have.
     */
    if (node->ss.ps.chgParam != NULL)
    {
        fsstate->cursor_exists = false;
        snprintf(sql, sizeof(sql), "CLOSE c%u",
                 fsstate->cursor_number);
    }
    else if (fsstate->fetch_ct_2 > 1)
    {
        snprintf(sql, sizeof(sql), "MOVE BACKWARD ALL IN c%u",
                 fsstate->cursor_number);
    }
    else
    {
        /* Easy: just rescan what we already have in memory, if anything */
        fsstate->next_tuple = 0;
        return;
    }
    
    /*
     * We don't use a PG_TRY block here, so be careful not to throw error
     * without releasing the PGresult.
     */
    res = pgfdw_exec_query(fsstate->conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pgfdw_report_error(ERROR, res, fsstate->conn, true, sql);
    PQclear(res);
    
    /* Now force a fresh FETCH. */
    fsstate->tuples = NULL;
    fsstate->num_tuples = 0;
    fsstate->next_tuple = 0;
    fsstate->fetch_ct_2 = 0;
    fsstate->eof_reached = false;
}

/*
 * postgresEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
postgresEndForeignScan(ForeignScanState *node)
{
    PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;
    
    //elog(INFO, "%s (node=%p", __func__, node);
    
    /* if fsstate is NULL, we are in EXPLAIN; nothing to do */
    if (fsstate == NULL)
        return;
    
    /* Close the cursor if open, to prevent accumulation of cursors */
    if (fsstate->cursor_exists)
        close_cursor(fsstate->conn, fsstate->cursor_number);
    
    /* Release remote connection */
    ReleaseConnection(fsstate->conn);
    fsstate->conn = NULL;
    
    /* MemoryContexts will be deleted automatically. */
}

/*
 * postgresRecheckForeignScan
 *		Execute a local join execution plan for a foreign join
 */
static bool
postgresRecheckForeignScan(ForeignScanState *node, TupleTableSlot *slot)
{
    Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;
    PlanState  *outerPlan = outerPlanState(node);
    TupleTableSlot *result;
    
    //elog(INFO, "%s (node=%p, slot=%p", __func__, node, slot);
    
    /* For base foreign relations, it suffices to set fdw_recheck_quals */
    if (scanrelid > 0)
        return true;
    
    Assert(outerPlan != NULL);
    
    /* Execute a local join execution plan */
    result = ExecProcNode(outerPlan);
    if (TupIsNull(result))
        return false;
    
    /* Store result in the given slot */
    ExecCopySlot(slot, result);
    
    return true;
}

/*
 * postgresExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
postgresExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    List	   *fdw_private;
    char	   *sql;
    char	   *relations;
    
    elog(INFO, "%s (node=%p, es=%p", __func__, node, es);
    
    fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
    
    /*
     * Add names of relation handled by the foreign scan when the scan is a
     * join
     */
    if (list_length(fdw_private) > FdwScanPrivateRelations)
    {
        relations = strVal(list_nth(fdw_private, FdwScanPrivateRelations));
        ExplainPropertyText("Relations", relations, es);
    }
    
    /*
     * Add remote query, when VERBOSE option is specified.
     */
    if (es->verbose)
    {
        sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
        ExplainPropertyText("Remote SQL", sql, es);
    }
}

/*
 * estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations or an upper
 *		relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 *
 * The function returns the cost and size estimates in p_row, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
estimate_path_cost_size(PlannerInfo *root,
                        RelOptInfo *foreignrel,
                        List *param_join_conds,
                        List *pathkeys,
                        double *p_rows, int *p_width,
                        Cost *p_startup_cost, Cost *p_total_cost)
{
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;
    double		rows;
    double		retrieved_rows;
    int			width;
    Cost		startup_cost;
    Cost		total_cost;
    Cost		cpu_per_tuple;
    
    /*
     * If the table or the server is configured to use remote estimates,
     * connect to the foreign server and execute EXPLAIN to estimate the
     * number of rows selected by the restriction+join clauses.  Otherwise,
     * estimate rows using whatever statistics we have locally, in a way
     * similar to ordinary tables.
     */
    if (fpinfo->use_remote_estimate)
    {
        List	   *remote_param_join_conds;
        List	   *local_param_join_conds;
        StringInfoData sql;
        PGconn	   *conn;
        Selectivity local_sel;
        QualCost	local_cost;
        List	   *fdw_scan_tlist = NIL;
        List	   *remote_conds;
        
        /* Required only to be passed to deparseSelectStmtForRel */
        List	   *retrieved_attrs;
        
        /*
         * param_join_conds might contain both clauses that are safe to send
         * across, and clauses that aren't.
         */
        classifyConditions(root, foreignrel, param_join_conds,
                           &remote_param_join_conds, &local_param_join_conds);
        
        /* Build the list of columns to be fetched from the foreign server. */
        if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
            fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
        else
            fdw_scan_tlist = NIL;
        
        /*
         * The complete list of remote conditions includes everything from
         * baserestrictinfo plus any extra join_conds relevant to this
         * particular path.
         */
        remote_conds = list_concat(list_copy(remote_param_join_conds),
                                   fpinfo->remote_conds);
        
        /*
         * Construct EXPLAIN query including the desired SELECT, FROM, and
         * WHERE clauses. Params and other-relation Vars are replaced by dummy
         * values, so don't request params_list.
         */
        initStringInfo(&sql);
        appendStringInfoString(&sql, "EXPLAIN ");
        deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
                                remote_conds, pathkeys, false,
                                &retrieved_attrs, NULL);
        
        /* Get the remote estimate */
        conn = GetConnection(fpinfo->user, false);
        get_remote_estimate(sql.data, conn, &rows, &width,
                            &startup_cost, &total_cost);
        ReleaseConnection(conn);
        
        retrieved_rows = rows;
        
        /* Factor in the selectivity of the locally-checked quals */
        local_sel = clauselist_selectivity(root,
                                           local_param_join_conds,
                                           (int) foreignrel->relid,
                                           JOIN_INNER,
                                           NULL);
        local_sel *= fpinfo->local_conds_sel;
        
        rows = clamp_row_est(rows * local_sel);
        
        /* Add in the eval cost of the locally-checked quals */
        startup_cost += fpinfo->local_conds_cost.startup;
        total_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
        cost_qual_eval(&local_cost, local_param_join_conds, root);
        startup_cost += local_cost.startup;
        total_cost += local_cost.per_tuple * retrieved_rows;
    }
    else
    {
        Cost		run_cost = 0;
        
        /*
         * We don't support join conditions in this mode (hence, no
         * parameterized paths can be made).
         */
        Assert(param_join_conds == NIL);
        
        /*
         * Use rows/width estimates made by set_baserel_size_estimates() for
         * base foreign relations and set_joinrel_size_estimates() for join
         * between foreign relations.
         */
        rows = foreignrel->rows;
        width = foreignrel->reltarget->width;
        
        /* Back into an estimate of the number of retrieved rows. */
        retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
        
        /*
         * We will come here again and again with different set of pathkeys
         * that caller wants to cost. We don't need to calculate the cost of
         * bare scan each time. Instead, use the costs if we have cached them
         * already.
         */
        if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0)
        {
            startup_cost = fpinfo->rel_startup_cost;
            run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
        }
        else if (IS_JOIN_REL(foreignrel))
        {
            PgFdwRelationInfo *fpinfo_i;
            PgFdwRelationInfo *fpinfo_o;
            QualCost	join_cost;
            QualCost	remote_conds_cost;
            double		nrows;
            
            /* For join we expect inner and outer relations set */
            Assert(fpinfo->innerrel && fpinfo->outerrel);
            
            fpinfo_i = (PgFdwRelationInfo *) fpinfo->innerrel->fdw_private;
            fpinfo_o = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;
            
            /* Estimate of number of rows in cross product */
            nrows = fpinfo_i->rows * fpinfo_o->rows;
            /* Clamp retrieved rows estimate to at most size of cross product */
            retrieved_rows = Min(retrieved_rows, nrows);
            
            /*
             * The cost of foreign join is estimated as cost of generating
             * rows for the joining relations + cost for applying quals on the
             * rows.
             */
            
            /*
             * Calculate the cost of clauses pushed down to the foreign server
             */
            cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
            /* Calculate the cost of applying join clauses */
            cost_qual_eval(&join_cost, fpinfo->joinclauses, root);
            
            /*
             * Startup cost includes startup cost of joining relations and the
             * startup cost for join and other clauses. We do not include the
             * startup cost specific to join strategy (e.g. setting up hash
             * tables) since we do not know what strategy the foreign server
             * is going to use.
             */
            startup_cost = fpinfo_i->rel_startup_cost + fpinfo_o->rel_startup_cost;
            startup_cost += join_cost.startup;
            startup_cost += remote_conds_cost.startup;
            startup_cost += fpinfo->local_conds_cost.startup;
            
            /*
             * Run time cost includes:
             *
             * 1. Run time cost (total_cost - startup_cost) of relations being
             * joined
             *
             * 2. Run time cost of applying join clauses on the cross product
             * of the joining relations.
             *
             * 3. Run time cost of applying pushed down other clauses on the
             * result of join
             *
             * 4. Run time cost of applying nonpushable other clauses locally
             * on the result fetched from the foreign server.
             */
            run_cost = fpinfo_i->rel_total_cost - fpinfo_i->rel_startup_cost;
            run_cost += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;
            run_cost += nrows * join_cost.per_tuple;
            nrows = clamp_row_est(nrows * fpinfo->joinclause_sel);
            run_cost += nrows * remote_conds_cost.per_tuple;
            run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
        }
        else if (IS_UPPER_REL(foreignrel))
        {
            PgFdwRelationInfo *ofpinfo;
            PathTarget *ptarget = root->upper_targets[UPPERREL_GROUP_AGG];
            AggClauseCosts aggcosts;
            double		input_rows;
            int			numGroupCols;
            double		numGroups = 1;
            
            /*
             * This cost model is mixture of costing done for sorted and
             * hashed aggregates in cost_agg().  We are not sure which
             * strategy will be considered at remote side, thus for
             * simplicity, we put all startup related costs in startup_cost
             * and all finalization and run cost are added in total_cost.
             *
             * Also, core does not care about costing HAVING expressions and
             * adding that to the costs.  So similarly, here too we are not
             * considering remote and local conditions for costing.
             */
            
            ofpinfo = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;
            
            /* Get rows and width from input rel */
            input_rows = ofpinfo->rows;
            width = ofpinfo->width;
            
            /* Collect statistics about aggregates for estimating costs. */
            MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
            if (root->parse->hasAggs)
            {
                get_agg_clause_costs(root, (Node *) fpinfo->grouped_tlist,
                                     AGGSPLIT_SIMPLE, &aggcosts);
                get_agg_clause_costs(root, (Node *) root->parse->havingQual,
                                     AGGSPLIT_SIMPLE, &aggcosts);
            }
            
            /* Get number of grouping columns and possible number of groups */
            numGroupCols = list_length(root->parse->groupClause);
            numGroups = estimate_num_groups(root,
                                            get_sortgrouplist_exprs(root->parse->groupClause,
                                                                    fpinfo->grouped_tlist),
                                            input_rows, NULL);
            
            /*
             * Number of rows expected from foreign server will be same as
             * that of number of groups.
             */
            rows = retrieved_rows = numGroups;
            
            /*-----
             * Startup cost includes:
             *	  1. Startup cost for underneath input * relation
             *	  2. Cost of performing aggregation, per cost_agg()
             *	  3. Startup cost for PathTarget eval
             *-----
             */
            startup_cost = ofpinfo->rel_startup_cost;
            startup_cost += aggcosts.transCost.startup;
            startup_cost += aggcosts.transCost.per_tuple * input_rows;
            startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;
            startup_cost += ptarget->cost.startup;
            
            /*-----
             * Run time cost includes:
             *	  1. Run time cost of underneath input relation
             *	  2. Run time cost of performing aggregation, per cost_agg()
             *	  3. PathTarget eval cost for each output row
             *-----
             */
            run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
            run_cost += aggcosts.finalCost * numGroups;
            run_cost += cpu_tuple_cost * numGroups;
            run_cost += ptarget->cost.per_tuple * numGroups;
        }
        else
        {
            /* Clamp retrieved rows estimates to at most foreignrel->tuples. */
            retrieved_rows = Min(retrieved_rows, foreignrel->tuples);
            
            /*
             * Cost as though this were a seqscan, which is pessimistic.  We
             * effectively imagine the local_conds are being evaluated
             * remotely, too.
             */
            startup_cost = 0;
            run_cost = 0;
            run_cost += seq_page_cost * foreignrel->pages;
            
            startup_cost += foreignrel->baserestrictcost.startup;
            cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
            run_cost += cpu_per_tuple * foreignrel->tuples;
        }
        
        /*
         * Without remote estimates, we have no real way to estimate the cost
         * of generating sorted output.  It could be free if the query plan
         * the remote side would have chosen generates properly-sorted output
         * anyway, but in most cases it will cost something.  Estimate a value
         * high enough that we won't pick the sorted path when the ordering
         * isn't locally useful, but low enough that we'll err on the side of
         * pushing down the ORDER BY clause when it's useful to do so.
         */
        if (pathkeys != NIL)
        {
            startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
            run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
        }
        
        total_cost = startup_cost + run_cost;
    }
    
    /*
     * Cache the costs for scans without any pathkeys or parameterization
     * before adding the costs for transferring data from the foreign server.
     * These costs are useful for costing the join between this relation and
     * another foreign relation or to calculate the costs of paths with
     * pathkeys for this relation, when the costs can not be obtained from the
     * foreign server. This function will be called at least once for every
     * foreign relation without pathkeys and parameterization.
     */
    if (pathkeys == NIL && param_join_conds == NIL)
    {
        fpinfo->rel_startup_cost = startup_cost;
        fpinfo->rel_total_cost = total_cost;
    }
    
    /*
     * Add some additional cost factors to account for connection overhead
     * (fdw_startup_cost), transferring data across the network
     * (fdw_tuple_cost per retrieved row), and local manipulation of the data
     * (cpu_tuple_cost per retrieved row).
     */
    startup_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
    total_cost += cpu_tuple_cost * retrieved_rows;
    
    /* Return results. */
    *p_rows = rows;
    *p_width = width;
    *p_startup_cost = startup_cost;
    *p_total_cost = total_cost;
}

/*
 * Estimate costs of executing a SQL statement remotely.
 * The given "sql" must be an EXPLAIN command.
 */
static void
get_remote_estimate(const char *sql, PGconn *conn,
                    double *rows, int *width,
                    Cost *startup_cost, Cost *total_cost)
{
    PGresult   *volatile res = NULL;
    
    /* PGresult must be released before leaving this function. */
    PG_TRY();
    {
        char	   *line;
        char	   *p;
        int			n;
        
        /*
         * Execute EXPLAIN remotely.
         */
        res = pgfdw_exec_query(conn, sql);
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
            pgfdw_report_error(ERROR, res, conn, false, sql);
        
        /*
         * Extract cost numbers for topmost plan node.  Note we search for a
         * left paren from the end of the line to avoid being confused by
         * other uses of parentheses.
         */
        line = PQgetvalue(res, 0, 0);
        p = strrchr(line, '(');
        if (p == NULL)
            elog(ERROR, "could not interpret EXPLAIN output: \"%s\"", line);
        n = sscanf(p, "(cost=%lf..%lf rows=%lf width=%d)",
                   startup_cost, total_cost, rows, width);
        if (n != 4)
            elog(ERROR, "could not interpret EXPLAIN output: \"%s\"", line);
        
        PQclear(res);
        res = NULL;
    }
    PG_CATCH();
    {
        if (res)
            PQclear(res);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * Detect whether we want to process an EquivalenceClass member.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
static bool
ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
                          EquivalenceClass *ec, EquivalenceMember *em,
                          void *arg)
{
    ec_member_foreign_arg *state = (ec_member_foreign_arg *) arg;
    Expr	   *expr = em->em_expr;
    
    /*
     * If we've identified what we're processing in the current scan, we only
     * want to match that expression.
     */
    if (state->current != NULL)
        return equal(expr, state->current);
    
    /*
     * Otherwise, ignore anything we've already processed.
     */
    if (list_member(state->already_used, expr))
        return false;
    
    /* This is the new target to process. */
    state->current = expr;
    return true;
}

/*
 * Create cursor for node's query with current parameter values.
 */
static void
create_cursor(ForeignScanState *node)
{
    PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    int			numParams = fsstate->numParams;
    const char **values = fsstate->param_values;
    PGconn	   *conn = fsstate->conn;
    StringInfoData buf;
    PGresult   *res;
    
    /*
     * Construct array of query parameter values in text format.  We do the
     * conversions in the short-lived per-tuple context, so as not to cause a
     * memory leak over repeated scans.
     */
    if (numParams > 0)
    {
        MemoryContext oldcontext;
        
        oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        
        process_query_params(econtext,
                             fsstate->param_flinfo,
                             fsstate->param_exprs,
                             values);
        
        MemoryContextSwitchTo(oldcontext);
    }
    
    /* Construct the DECLARE CURSOR command */
    initStringInfo(&buf);
    appendStringInfo(&buf, "DECLARE c%u CURSOR FOR\n%s",
                     fsstate->cursor_number, fsstate->query);
    
    /*
     * Notice that we pass NULL for paramTypes, thus forcing the remote server
     * to infer types for all parameters.  Since we explicitly cast every
     * parameter (see deparse.c), the "inference" is trivial and will produce
     * the desired result.  This allows us to avoid assuming that the remote
     * server has the same OIDs we do for the parameters' types.
     */
    if (!PQsendQueryParams(conn, buf.data, numParams,
                           NULL, values, NULL, NULL, 0))
        pgfdw_report_error(ERROR, NULL, conn, false, buf.data);
    
    /*
     * Get the result, and check for success.
     *
     * We don't use a PG_TRY block here, so be careful not to throw error
     * without releasing the PGresult.
     */
    res = pgfdw_get_result(conn, buf.data);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pgfdw_report_error(ERROR, res, conn, true, fsstate->query);
    PQclear(res);
    
    /* Mark the cursor as created, and show no tuples have been retrieved */
    fsstate->cursor_exists = true;
    fsstate->tuples = NULL;
    fsstate->num_tuples = 0;
    fsstate->next_tuple = 0;
    fsstate->fetch_ct_2 = 0;
    fsstate->eof_reached = false;
    
    /* Clean up */
    pfree(buf.data);
}

/*
 * Fetch some more rows from the node's cursor.
 */
static void
fetch_more_data(ForeignScanState *node)
{
    PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;
    PGresult   *volatile res = NULL;
    MemoryContext oldcontext;
    
    /*
     * We'll store the tuples in the batch_cxt.  First, flush the previous
     * batch.
     */
    fsstate->tuples = NULL;
    MemoryContextReset(fsstate->batch_cxt);
    oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt);
    
    /* PGresult must be released before leaving this function. */
    PG_TRY();
    {
        PGconn	   *conn = fsstate->conn;
        char		sql[64];
        int			numrows;
        int			i;
        
        snprintf(sql, sizeof(sql), "FETCH %d FROM c%u",
                 fsstate->fetch_size, fsstate->cursor_number);
        
        res = pgfdw_exec_query(conn, sql);
        /* On error, report the original query, not the FETCH. */
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
            pgfdw_report_error(ERROR, res, conn, false, fsstate->query);
        
        /* Convert the data into HeapTuples */
        numrows = PQntuples(res);
        fsstate->tuples = (HeapTuple *) palloc0((size_t) numrows * sizeof(HeapTuple));
        fsstate->num_tuples = numrows;
        fsstate->next_tuple = 0;
        
        for (i = 0; i < numrows; i++)
        {
            Assert(IsA(node->ss.ps.plan, ForeignScan));
            
            fsstate->tuples[i] =
            make_tuple_from_result_row(res, i,
                                       fsstate->rel,
                                       fsstate->attinmeta,
                                       fsstate->retrieved_attrs,
                                       node,
                                       fsstate->temp_cxt);
        }
        
        /* Update fetch_ct_2 */
        if (fsstate->fetch_ct_2 < 2)
            fsstate->fetch_ct_2++;
        
        /* Must be EOF if we didn't get as many tuples as we asked for. */
        fsstate->eof_reached = (numrows < fsstate->fetch_size);
        
        PQclear(res);
        res = NULL;
    }
    PG_CATCH();
    {
        if (res)
            PQclear(res);
        PG_RE_THROW();
    }
    PG_END_TRY();
    
    MemoryContextSwitchTo(oldcontext);
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int
set_transmission_modes(void)
{
    int			nestlevel = NewGUCNestLevel();
    
    /*
     * The values set here should match what pg_dump does.  See also
     * configure_remote_session in connection.c.
     */
    if (DateStyle != USE_ISO_DATES)
        (void) set_config_option("datestyle", "ISO",
                                 PGC_USERSET, PGC_S_SESSION,
                                 GUC_ACTION_SAVE, true, 0, false);
    if (IntervalStyle != INTSTYLE_POSTGRES)
        (void) set_config_option("intervalstyle", "postgres",
                                 PGC_USERSET, PGC_S_SESSION,
                                 GUC_ACTION_SAVE, true, 0, false);
    if (extra_float_digits < 3)
        (void) set_config_option("extra_float_digits", "3",
                                 PGC_USERSET, PGC_S_SESSION,
                                 GUC_ACTION_SAVE, true, 0, false);
    
    return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void
reset_transmission_modes(int nestlevel)
{
    AtEOXact_GUC(true, nestlevel);
}

/*
 * Utility routine to close a cursor.
 */
static void
close_cursor(PGconn *conn, unsigned int cursor_number)
{
    char		sql[64];
    PGresult   *res;
    
    snprintf(sql, sizeof(sql), "CLOSE c%u", cursor_number);
    
    /*
     * We don't use a PG_TRY block here, so be careful not to throw error
     * without releasing the PGresult.
     */
    res = pgfdw_exec_query(conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pgfdw_report_error(ERROR, res, conn, true, sql);
    PQclear(res);
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node,
                     List *fdw_exprs,
                     int numParams,
                     FmgrInfo **param_flinfo,
                     List **param_exprs,
                     const char ***param_values)
{
    int			i;
    ListCell   *lc;
    
    Assert(numParams > 0);
    
    /* Prepare for output conversion of parameters used in remote query. */
    *param_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * (size_t) numParams);
    
    i = 0;
    foreach(lc, fdw_exprs)
    {
        Node	   *param_expr = (Node *) lfirst(lc);
        Oid			typefnoid;
        bool		isvarlena;
        
        getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
        fmgr_info(typefnoid, &(*param_flinfo)[i]);
        i++;
    }
    
    /*
     * Prepare remote-parameter expressions for evaluation.  (Note: in
     * practice, we expect that all these expressions will be just Params, so
     * we could possibly do something more efficient than using the full
     * expression-eval machinery for this.  But probably there would be little
     * benefit, and it'd require postgres_fdw to know more than is desirable
     * about Param evaluation.)
     */
    *param_exprs = ExecInitExprList(fdw_exprs, node);
    
    /* Allocate buffer for text form of query parameters. */
    *param_values = (const char **) palloc0((size_t) numParams * sizeof(char *));
}

/*
 * Construct array of query parameter values in text format.
 */
static void
process_query_params(ExprContext *econtext,
                     FmgrInfo *param_flinfo,
                     List *param_exprs,
                     const char **param_values)
{
    int			nestlevel;
    int			i;
    ListCell   *lc;
    
    nestlevel = set_transmission_modes();
    
    i = 0;
    foreach(lc, param_exprs)
    {
        ExprState  *expr_state = (ExprState *) lfirst(lc);
        Datum		expr_value;
        bool		isNull;
        
        /* Evaluate the parameter expression */
        expr_value = ExecEvalExpr(expr_state, econtext, &isNull);
        
        /*
         * Get string representation of each parameter value by invoking
         * type-specific output function, unless the value is null.
         */
        if (isNull)
            param_values[i] = NULL;
        else
            param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);
        
        i++;
    }
    
    reset_transmission_modes(nestlevel);
}

/*
 * Import a foreign schema
 */
static List *
postgresImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    List	   *commands = NIL;
    bool		import_collate = true;
    bool		import_default = false;
    bool		import_not_null = true;
    ForeignServer *server;
    UserMapping *mapping;
    PGconn	   *conn;
    StringInfoData buf;
    PGresult   *volatile res = NULL;
    int			numrows,
				i;
    ListCell   *lc;
    
    /* Parse statement options */
    foreach(lc, stmt->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);
        
        if (strcmp(def->defname, "import_collate") == 0)
            import_collate = defGetBoolean(def);
        else if (strcmp(def->defname, "import_default") == 0)
            import_default = defGetBoolean(def);
        else if (strcmp(def->defname, "import_not_null") == 0)
            import_not_null = defGetBoolean(def);
        else
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("invalid option \"%s\"", def->defname)));
    }
    
    /*
     * Get connection to the foreign server.  Connection manager will
     * establish new connection if necessary.
     */
    server = GetForeignServer(serverOid);
    mapping = GetUserMapping(GetUserId(), server->serverid);
    conn = GetConnection(mapping, false);
    
    /* Don't attempt to import collation if remote server hasn't got it */
    if (PQserverVersion(conn) < 90100)
        import_collate = false;
    
    /* Create workspace for strings */
    initStringInfo(&buf);
    
    /* In what follows, do not risk leaking any PGresults. */
    PG_TRY();
    {
        /* Check that the schema really exists */
        appendStringInfoString(&buf, "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ");
        deparseStringLiteral(&buf, stmt->remote_schema);
        
        res = pgfdw_exec_query(conn, buf.data);
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
            pgfdw_report_error(ERROR, res, conn, false, buf.data);
        
        if (PQntuples(res) != 1)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND),
                     errmsg("schema \"%s\" is not present on foreign server \"%s\"",
                            stmt->remote_schema, server->servername)));
        
        PQclear(res);
        res = NULL;
        resetStringInfo(&buf);
        
        /*
         * Fetch all table data from this schema, possibly restricted by
         * EXCEPT or LIMIT TO.  (We don't actually need to pay any attention
         * to EXCEPT/LIMIT TO here, because the core code will filter the
         * statements we return according to those lists anyway.  But it
         * should save a few cycles to not process excluded tables in the
         * first place.)
         *
         * Ignore table data for partitions and only include the definitions
         * of the root partitioned tables to allow access to the complete
         * remote data set locally in the schema imported.
         *
         * Note: because we run the connection with search_path restricted to
         * pg_catalog, the format_type() and pg_get_expr() outputs will always
         * include a schema name for types/functions in other schemas, which
         * is what we want.
         */
        if (import_collate)
            appendStringInfoString(&buf,
                                   "SELECT relname, "
                                   "  attname, "
                                   "  format_type(atttypid, atttypmod), "
                                   "  attnotnull, "
                                   "  pg_get_expr(adbin, adrelid), "
                                   "  collname, "
                                   "  collnsp.nspname "
                                   "FROM pg_class c "
                                   "  JOIN pg_namespace n ON "
                                   "    relnamespace = n.oid "
                                   "  LEFT JOIN pg_attribute a ON "
                                   "    attrelid = c.oid AND attnum > 0 "
                                   "      AND NOT attisdropped "
                                   "  LEFT JOIN pg_attrdef ad ON "
                                   "    adrelid = c.oid AND adnum = attnum "
                                   "  LEFT JOIN pg_collation coll ON "
                                   "    coll.oid = attcollation "
                                   "  LEFT JOIN pg_namespace collnsp ON "
                                   "    collnsp.oid = collnamespace ");
        else
            appendStringInfoString(&buf,
                                   "SELECT relname, "
                                   "  attname, "
                                   "  format_type(atttypid, atttypmod), "
                                   "  attnotnull, "
                                   "  pg_get_expr(adbin, adrelid), "
                                   "  NULL, NULL "
                                   "FROM pg_class c "
                                   "  JOIN pg_namespace n ON "
                                   "    relnamespace = n.oid "
                                   "  LEFT JOIN pg_attribute a ON "
                                   "    attrelid = c.oid AND attnum > 0 "
                                   "      AND NOT attisdropped "
                                   "  LEFT JOIN pg_attrdef ad ON "
                                   "    adrelid = c.oid AND adnum = attnum ");
        
        appendStringInfoString(&buf,
                               "WHERE c.relkind IN ("
                               CppAsString2(RELKIND_RELATION) ","
                               CppAsString2(RELKIND_VIEW) ","
                               CppAsString2(RELKIND_FOREIGN_TABLE) ","
                               CppAsString2(RELKIND_MATVIEW) ","
                               CppAsString2(RELKIND_PARTITIONED_TABLE) ") "
                               "  AND n.nspname = ");
        deparseStringLiteral(&buf, stmt->remote_schema);
        
        /* Partitions are supported since Postgres 10 */
        if (PQserverVersion(conn) >= 100000)
            appendStringInfoString(&buf, " AND NOT c.relispartition ");
        
        /* Apply restrictions for LIMIT TO and EXCEPT */
        if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
            stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
        {
            bool		first_item = true;
            
            appendStringInfoString(&buf, " AND c.relname ");
            if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
                appendStringInfoString(&buf, "NOT ");
            appendStringInfoString(&buf, "IN (");
            
            /* Append list of table names within IN clause */
            foreach(lc, stmt->table_list)
            {
                RangeVar   *rv = (RangeVar *) lfirst(lc);
                
                if (first_item)
                    first_item = false;
                else
                    appendStringInfoString(&buf, ", ");
                deparseStringLiteral(&buf, rv->relname);
            }
            appendStringInfoChar(&buf, ')');
        }
        
        /* Append ORDER BY at the end of query to ensure output ordering */
        appendStringInfoString(&buf, " ORDER BY c.relname, a.attnum");
        
        /* Fetch the data */
        res = pgfdw_exec_query(conn, buf.data);
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
            pgfdw_report_error(ERROR, res, conn, false, buf.data);
        
        /* Process results */
        numrows = PQntuples(res);
        /* note: incrementation of i happens in inner loop's while() test */
        for (i = 0; i < numrows;)
        {
            char	   *tablename = PQgetvalue(res, i, 0);
            bool		first_item = true;
            
            resetStringInfo(&buf);
            appendStringInfo(&buf, "CREATE FOREIGN TABLE %s (\n",
                             quote_identifier(tablename));
            
            /* Scan all rows for this table */
            do
            {
                char	   *attname;
                char	   *typename;
                char	   *attnotnull;
                char	   *attdefault;
                char	   *collname;
                char	   *collnamespace;
                
                /* If table has no columns, we'll see nulls here */
                if (PQgetisnull(res, i, 1))
                    continue;
                
                attname = PQgetvalue(res, i, 1);
                typename = PQgetvalue(res, i, 2);
                attnotnull = PQgetvalue(res, i, 3);
                attdefault = PQgetisnull(res, i, 4) ? (char *) NULL :
                PQgetvalue(res, i, 4);
                collname = PQgetisnull(res, i, 5) ? (char *) NULL :
                PQgetvalue(res, i, 5);
                collnamespace = PQgetisnull(res, i, 6) ? (char *) NULL :
                PQgetvalue(res, i, 6);
                
                if (first_item)
                    first_item = false;
                else
                    appendStringInfoString(&buf, ",\n");
                
                /* Print column name and type */
                appendStringInfo(&buf, "  %s %s",
                                 quote_identifier(attname),
                                 typename);
                
                /*
                 * Add column_name option so that renaming the foreign table's
                 * column doesn't break the association to the underlying
                 * column.
                 */
                appendStringInfoString(&buf, " OPTIONS (column_name ");
                deparseStringLiteral(&buf, attname);
                appendStringInfoChar(&buf, ')');
                
                /* Add COLLATE if needed */
                if (import_collate && collname != NULL && collnamespace != NULL)
                    appendStringInfo(&buf, " COLLATE %s.%s",
                                     quote_identifier(collnamespace),
                                     quote_identifier(collname));
                
                /* Add DEFAULT if needed */
                if (import_default && attdefault != NULL)
                    appendStringInfo(&buf, " DEFAULT %s", attdefault);
                
                /* Add NOT NULL if needed */
                if (import_not_null && attnotnull[0] == 't')
                    appendStringInfoString(&buf, " NOT NULL");
            }
            while (++i < numrows &&
                   strcmp(PQgetvalue(res, i, 0), tablename) == 0);
            
            /*
             * Add server name and table-level options.  We specify remote
             * schema and table name as options (the latter to ensure that
             * renaming the foreign table doesn't break the association).
             */
            appendStringInfo(&buf, "\n) SERVER %s\nOPTIONS (",
                             quote_identifier(server->servername));
            
            appendStringInfoString(&buf, "schema_name ");
            deparseStringLiteral(&buf, stmt->remote_schema);
            appendStringInfoString(&buf, ", table_name ");
            deparseStringLiteral(&buf, tablename);
            
            appendStringInfoString(&buf, ");");
            
            commands = lappend(commands, pstrdup(buf.data));
        }
        
        /* Clean up */
        PQclear(res);
        res = NULL;
    }
    PG_CATCH();
    {
        if (res)
            PQclear(res);
        PG_RE_THROW();
    }
    PG_END_TRY();
    
    ReleaseConnection(conn);
    
    return commands;
}

/*
 * Assess whether the join between inner and outer relations can be pushed down
 * to the foreign server. As a side effect, save information we obtain in this
 * function to PgFdwRelationInfo passed in.
 */
static bool
foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
                RelOptInfo *outerrel, RelOptInfo *innerrel,
                JoinPathExtraData *extra)
{
    PgFdwRelationInfo *fpinfo;
    PgFdwRelationInfo *fpinfo_o;
    PgFdwRelationInfo *fpinfo_i;
    ListCell   *lc;
    List	   *joinclauses;
    
    /*
     * We support pushing down INNER, LEFT, RIGHT and FULL OUTER joins.
     * Constructing queries representing SEMI and ANTI joins is hard, hence
     * not considered right now.
     */
    if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
        jointype != JOIN_RIGHT && jointype != JOIN_FULL)
        return false;
    
    /*
     * If either of the joining relations is marked as unsafe to pushdown, the
     * join can not be pushed down.
     */
    fpinfo = (PgFdwRelationInfo *) joinrel->fdw_private;
    fpinfo_o = (PgFdwRelationInfo *) outerrel->fdw_private;
    fpinfo_i = (PgFdwRelationInfo *) innerrel->fdw_private;
    if (!fpinfo_o || !fpinfo_o->pushdown_safe ||
        !fpinfo_i || !fpinfo_i->pushdown_safe)
        return false;
    
    /*
     * If joining relations have local conditions, those conditions are
     * required to be applied before joining the relations. Hence the join can
     * not be pushed down.
     */
    if (fpinfo_o->local_conds || fpinfo_i->local_conds)
    {
        elog(INFO, "%s: join not pushdown safe due to local conds: %s %s", __func__, nodeToString(fpinfo_i->local_conds), nodeToString(fpinfo_o->local_conds));

        return false;
    }
    
    /*
     * Merge FDW options.  We might be tempted to do this after we have deemed
     * the foreign join to be OK.  But we must do this beforehand so that we
     * know which quals can be evaluated on the foreign server, which might
     * depend on shippable_extensions.
     */
    fpinfo->server = fpinfo_o->server;
    merge_fdw_options(fpinfo, fpinfo_o, fpinfo_i);
    
    /*
     * Separate restrict list into join quals and pushed-down (other) quals.
     *
     * Join quals belonging to an outer join must all be shippable, else we
     * cannot execute the join remotely.  Add such quals to 'joinclauses'.
     *
     * Add other quals to fpinfo->remote_conds if they are shippable, else to
     * fpinfo->local_conds.  In an inner join it's okay to execute conditions
     * either locally or remotely; the same is true for pushed-down conditions
     * at an outer join.
     *
     * Note we might return failure after having already scribbled on
     * fpinfo->remote_conds and fpinfo->local_conds.  That's okay because we
     * won't consult those lists again if we deem the join unshippable.
     */
    joinclauses = NIL;
    foreach(lc, extra->restrictlist)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
        bool		is_remote_clause = is_foreign_expr(root, joinrel,
                                                       rinfo->clause);
        
        if (IS_OUTER_JOIN(jointype) && !rinfo->is_pushed_down)
        {
            if (!is_remote_clause)
                return false;
            joinclauses = lappend(joinclauses, rinfo);
        }
        else
        {
            if (is_remote_clause)
                fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
            else
                fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
        }
    }
    
    /*
     * deparseExplicitTargetList() isn't smart enough to handle anything other
     * than a Var.  In particular, if there's some PlaceHolderVar that would
     * need to be evaluated within this join tree (because there's an upper
     * reference to a quantity that may go to NULL as a result of an outer
     * join), then we can't try to push the join down because we'll fail when
     * we get to deparseExplicitTargetList().  However, a PlaceHolderVar that
     * needs to be evaluated *at the top* of this join tree is OK, because we
     * can do that locally after fetching the results from the remote side.
     */
    foreach(lc, root->placeholder_list)
    {
        PlaceHolderInfo *phinfo = lfirst(lc);
        Relids		relids = joinrel->relids;
        
        if (bms_is_subset(phinfo->ph_eval_at, relids) &&
            bms_nonempty_difference(relids, phinfo->ph_eval_at))
            return false;
    }
    
    /* Save the join clauses, for later use. */
    fpinfo->joinclauses = joinclauses;
    
    fpinfo->outerrel = outerrel;
    fpinfo->innerrel = innerrel;
    fpinfo->jointype = jointype;
    
    /*
     * By default, both the input relations are not required to be deparsed as
     * subqueries, but there might be some relations covered by the input
     * relations that are required to be deparsed as subqueries, so save the
     * relids of those relations for later use by the deparser.
     */
    fpinfo->make_outerrel_subquery = false;
    fpinfo->make_innerrel_subquery = false;
    Assert(bms_is_subset(fpinfo_o->lower_subquery_rels, outerrel->relids));
    Assert(bms_is_subset(fpinfo_i->lower_subquery_rels, innerrel->relids));
    fpinfo->lower_subquery_rels = bms_union(fpinfo_o->lower_subquery_rels,
                                            fpinfo_i->lower_subquery_rels);
    
    /*
     * Pull the other remote conditions from the joining relations into join
     * clauses or other remote clauses (remote_conds) of this relation
     * wherever possible. This avoids building subqueries at every join step.
     *
     * For an inner join, clauses from both the relations are added to the
     * other remote clauses. For LEFT and RIGHT OUTER join, the clauses from
     * the outer side are added to remote_conds since those can be evaluated
     * after the join is evaluated. The clauses from inner side are added to
     * the joinclauses, since they need to be evaluated while constructing the
     * join.
     *
     * For a FULL OUTER JOIN, the other clauses from either relation can not
     * be added to the joinclauses or remote_conds, since each relation acts
     * as an outer relation for the other.
     *
     * The joining sides can not have local conditions, thus no need to test
     * shippability of the clauses being pulled up.
     */
    switch (jointype)
    {
        case JOIN_INNER:
            fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
                                               list_copy(fpinfo_i->remote_conds));
            fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
                                               list_copy(fpinfo_o->remote_conds));
            break;
            
        case JOIN_LEFT:
            fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
                                              list_copy(fpinfo_i->remote_conds));
            fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
                                               list_copy(fpinfo_o->remote_conds));
            break;
            
        case JOIN_RIGHT:
            fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
                                              list_copy(fpinfo_o->remote_conds));
            fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
                                               list_copy(fpinfo_i->remote_conds));
            break;
            
        case JOIN_FULL:
            
            /*
             * In this case, if any of the input relations has conditions, we
             * need to deparse that relation as a subquery so that the
             * conditions can be evaluated before the join.  Remember it in
             * the fpinfo of this relation so that the deparser can take
             * appropriate action.  Also, save the relids of base relations
             * covered by that relation for later use by the deparser.
             */
            if (fpinfo_o->remote_conds)
            {
                fpinfo->make_outerrel_subquery = true;
                fpinfo->lower_subquery_rels =
                bms_add_members(fpinfo->lower_subquery_rels,
                                outerrel->relids);
            }
            if (fpinfo_i->remote_conds)
            {
                fpinfo->make_innerrel_subquery = true;
                fpinfo->lower_subquery_rels =
                bms_add_members(fpinfo->lower_subquery_rels,
                                innerrel->relids);
            }
            break;
            
        default:
            /* Should not happen, we have just checked this above */
            elog(ERROR, "unsupported join type %d", jointype);
    }
    
    /*
     * For an inner join, all restrictions can be treated alike. Treating the
     * pushed down conditions as join conditions allows a top level full outer
     * join to be deparsed without requiring subqueries.
     */
    if (jointype == JOIN_INNER)
    {
        Assert(!fpinfo->joinclauses);
        fpinfo->joinclauses = fpinfo->remote_conds;
        fpinfo->remote_conds = NIL;
    }
    
    /* Mark that this join can be pushed down safely */
    fpinfo->pushdown_safe = true;
    
    /* Get user mapping */
    if (fpinfo->use_remote_estimate)
    {
        if (fpinfo_o->use_remote_estimate)
            fpinfo->user = fpinfo_o->user;
        else
            fpinfo->user = fpinfo_i->user;
    }
    else
        fpinfo->user = NULL;
    
    /*
     * Set cached relation costs to some negative value, so that we can detect
     * when they are set to some sensible costs, during one (usually the
     * first) of the calls to estimate_path_cost_size().
     */
    fpinfo->rel_startup_cost = -1;
    fpinfo->rel_total_cost = -1;
    
    /*
     * Set the string describing this join relation to be used in EXPLAIN
     * output of corresponding ForeignScan.
     */
    fpinfo->relation_name = makeStringInfo();
    appendStringInfo(fpinfo->relation_name, "(%s) %s JOIN (%s)",
                     fpinfo_o->relation_name->data,
                     get_jointype_name(fpinfo->jointype),
                     fpinfo_i->relation_name->data);
    
    /*
     * Set the relation index.  This is defined as the position of this
     * joinrel in the join_rel_list list plus the length of the rtable list.
     * Note that since this joinrel is at the end of the join_rel_list list
     * when we are called, we can get the position by list_length.
     */
    Assert(fpinfo->relation_index == 0);	/* shouldn't be set yet */
    fpinfo->relation_index =
    list_length(root->parse->rtable) + list_length(root->join_rel_list);
    
    return true;
}

static void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
                                Path *epq_path)
{
    List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */
    ListCell   *lc;
    
    useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);
    
    /* Create one path for each set of pathkeys we found above. */
    foreach(lc, useful_pathkeys_list)
    {
        double		rows;
        int			width;
        Cost		startup_cost;
        Cost		total_cost;
        List	   *useful_pathkeys = lfirst(lc);
        
        estimate_path_cost_size(root, rel, NIL, useful_pathkeys,
                                &rows, &width, &startup_cost, &total_cost);
        
        add_path(rel, (Path *)
                 create_foreignscan_path(root, rel,
                                         NULL,
                                         rows,
                                         startup_cost,
                                         total_cost,
                                         useful_pathkeys,
                                         NULL,
                                         epq_path,
                                         NIL));
    }
}

/*
 * Parse options from foreign server and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_server_options(PgFdwRelationInfo *fpinfo)
{
    ListCell   *lc;
    
    foreach(lc, fpinfo->server->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);
        
        if (strcmp(def->defname, "use_remote_estimate") == 0)
            fpinfo->use_remote_estimate = defGetBoolean(def);
        else if (strcmp(def->defname, "fdw_startup_cost") == 0)
            fpinfo->fdw_startup_cost = strtod(defGetString(def), NULL);
        else if (strcmp(def->defname, "fdw_tuple_cost") == 0)
            fpinfo->fdw_tuple_cost = strtod(defGetString(def), NULL);
        else if (strcmp(def->defname, "extensions") == 0)
            fpinfo->shippable_extensions =
            ExtractExtensionList(defGetString(def), false);
        else if (strcmp(def->defname, "fetch_size") == 0)
            fpinfo->fetch_size = (int) strtol(defGetString(def), NULL, 10);
        else if (strcmp(def->defname, "trace_join_clause_check") == 0)
            fpinfo->trace_join_clause_check = defGetBoolean(def);
        else if (strcmp(def->defname, "debug_join_clause_check") == 0)
            fpinfo->debug_join_clause_check = defGetBoolean(def);
        else if (strcmp(def->defname, "trace_where_clause_source_check") == 0)
            fpinfo->trace_where_clause_source_check = defGetBoolean(def);
        else if (strcmp(def->defname, "trace_parse_select_query") == 0)
            fpinfo->trace_parse_select_query = defGetBoolean(def);
        else if (strcmp(def->defname, "trace_shippable_check") == 0)
            fpinfo->trace_shippable_check = defGetBoolean(def);
        else if (strcmp(def->defname, "trace_group_clause_source_check") == 0)
            fpinfo->trace_group_clause_source_check = defGetBoolean(def);
        else if (strcmp(def->defname, "trace_select_clause_source_check") == 0)
            fpinfo->trace_select_clause_source_check = defGetBoolean(def);
    }
}

/*
 * Parse options from foreign table and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_table_options(PgFdwRelationInfo *fpinfo)
{
    ListCell   *lc;
    
    foreach(lc, fpinfo->table->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);
        
        if (strcmp(def->defname, "use_remote_estimate") == 0)
            fpinfo->use_remote_estimate = defGetBoolean(def);
        else if (strcmp(def->defname, "fetch_size") == 0)
            fpinfo->fetch_size = (int) strtol(defGetString(def), NULL, 10);
    }
}

/*
 * Merge FDW options from input relations into a new set of options for a join
 * or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer
 * relations is provided using fpinfo_i and fpinfo_o.  For an upper relation,
 * fpinfo_o provides the information for the input relation; fpinfo_i is
 * expected to NULL.
 */
static void
merge_fdw_options(PgFdwRelationInfo *fpinfo,
                  const PgFdwRelationInfo *fpinfo_o,
                  const PgFdwRelationInfo *fpinfo_i)
{
    /* We must always have fpinfo_o. */
    Assert(fpinfo_o);
    
    /* fpinfo_i may be NULL, but if present the servers must both match. */
    Assert(!fpinfo_i ||
           fpinfo_i->server->serverid == fpinfo_o->server->serverid);
    
    /*
     * Copy the server specific FDW options.  (For a join, both relations come
     * from the same server, so the server options should have the same value
     * for both relations.)
     */
    fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
    fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
    fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
    fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate;
    fpinfo->fetch_size = fpinfo_o->fetch_size;
    
    fpinfo->debug_join_clause_check = fpinfo_o->debug_join_clause_check;
    fpinfo->trace_join_clause_check = fpinfo_o->trace_join_clause_check;
    fpinfo->trace_where_clause_source_check = fpinfo_o->trace_where_clause_source_check;
    fpinfo->trace_parse_select_query = fpinfo_o->trace_parse_select_query;
    fpinfo->trace_shippable_check = fpinfo_o->trace_shippable_check;
    fpinfo->trace_group_clause_source_check = fpinfo_o->trace_group_clause_source_check;
    fpinfo->trace_select_clause_source_check = fpinfo_o->trace_select_clause_source_check;

    /* Merge the table level options from either side of the join. */
    if (fpinfo_i)
    {
        /*
         * We'll prefer to use remote estimates for this join if any table
         * from either side of the join is using remote estimates.  This is
         * most likely going to be preferred since they're already willing to
         * pay the price of a round trip to get the remote EXPLAIN.  In any
         * case it's not entirely clear how we might otherwise handle this
         * best.
         */
        fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate ||
        fpinfo_i->use_remote_estimate;
        
        /*
         * Set fetch size to maximum of the joining sides, since we are
         * expecting the rows returned by the join to be proportional to the
         * relation sizes.
         */
        fpinfo->fetch_size = Max(fpinfo_o->fetch_size, fpinfo_i->fetch_size);
    }
}

void add_rewritten_mv_paths (PlannerInfo *root,
                             RelOptInfo *input_rel, // if grouping
                             RelOptInfo *inner_rel, RelOptInfo *outer_rel, // if joining
                             RelOptInfo *grouped_rel,
                             PgFdwRelationInfo *fpinfo, PathTarget *grouping_target);

/*
 * postgresGetForeignJoinPaths
 *		Add possible ForeignPath to joinrel, if join is safe to push down.
 */
static void
postgresGetForeignJoinPaths(PlannerInfo *root,
                            RelOptInfo *joinrel,
                            RelOptInfo *outerrel,
                            RelOptInfo *innerrel,
                            JoinType jointype,
                            JoinPathExtraData *extra)
{
    PgFdwRelationInfo *fpinfo;
    ForeignPath *joinpath;
    double		rows;
    int			width;
    Cost		startup_cost;
    Cost		total_cost;
    Path	   *epq_path;		/* Path to create plan to be executed when
                                 * EvalPlanQual gets triggered. */

    //elog(INFO, "%s (root=%p, joinrel=%p, outerrel=%p, innerrel=%p, jointype=%d, extra=%p)", __func__, root, joinrel, outerrel, innerrel, jointype, extra);

    /*
     * Skip if this join combination has been considered already.
     */
    if (joinrel->fdw_private)
        return;
    
    /*
     * Create unfinished PgFdwRelationInfo entry which is used to indicate
     * that the join relation is already considered, so that we won't waste
     * time in judging safety of join pushdown and adding the same paths again
     * if found safe. Once we know that this join can be pushed down, we fill
     * the entry.
     */
    fpinfo = (PgFdwRelationInfo *) palloc0(sizeof(PgFdwRelationInfo));
    fpinfo->pushdown_safe = false;
    joinrel->fdw_private = fpinfo;
    /* attrs_used is only for base relations. */
    fpinfo->attrs_used = NULL;
    
    /*
     * If there is a possibility that EvalPlanQual will be executed, we need
     * to be able to reconstruct the row using scans of the base relations.
     * GetExistingLocalJoinPath will find a suitable path for this purpose in
     * the path list of the joinrel, if one exists.  We must be careful to
     * call it before adding any ForeignPath, since the ForeignPath might
     * dominate the only suitable local path available.  We also do it before
     * calling foreign_join_ok(), since that function updates fpinfo and marks
     * it as pushable if the join is found to be pushable.
     */
    if (root->parse->commandType == CMD_DELETE ||
        root->parse->commandType == CMD_UPDATE ||
        root->rowMarks)
    {
        epq_path = GetExistingLocalJoinPath(joinrel);
        if (!epq_path)
        {
            elog(DEBUG3, "could not push down foreign join because a local path suitable for EPQ checks was not found");
            return;
        }
    }
    else
        epq_path = NULL;
    
    if (!foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra))
    {
        /* Free path required for EPQ if we copied one; we don't need it now */
        if (epq_path)
            pfree(epq_path);
        return;
    }
    
    /*
     * Compute the selectivity and cost of the local_conds, so we don't have
     * to do it over again for each path. The best we can do for these
     * conditions is to estimate selectivity on the basis of local statistics.
     * The local conditions are applied after the join has been computed on
     * the remote side like quals in WHERE clause, so pass jointype as
     * JOIN_INNER.
     */
    fpinfo->local_conds_sel = clauselist_selectivity(root,
                                                     fpinfo->local_conds,
                                                     0,
                                                     JOIN_INNER,
                                                     NULL);
    cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);
    
    /*
     * If we are going to estimate costs locally, estimate the join clause
     * selectivity here while we have special join info.
     */
    if (!fpinfo->use_remote_estimate)
        fpinfo->joinclause_sel = clauselist_selectivity(root, fpinfo->joinclauses,
                                                        0, fpinfo->jointype,
                                                        extra->sjinfo);
    
    /* Estimate costs for bare join relation */
    estimate_path_cost_size(root, joinrel, NIL, NIL, &rows,
                            &width, &startup_cost, &total_cost);
    /* Now update this information in the joinrel */
    joinrel->rows = rows;
    joinrel->reltarget->width = width;
    fpinfo->rows = rows;
    fpinfo->width = width;
    fpinfo->startup_cost = startup_cost;
    fpinfo->total_cost = total_cost;
    
    /*
     * Create a new join path and add it to the joinrel which represents a
     * join between foreign tables.
     */
    joinpath = create_foreignscan_path(root,
                                       joinrel,
                                       NULL,	/* default pathtarget */
                                       rows,
                                       startup_cost,
                                       total_cost,
                                       NIL, /* no pathkeys */
                                       NULL,	/* no required_outer */
                                       epq_path,
                                       NIL);	/* no fdw_private */
    
    /* Add generated path into joinrel by add_path(). */
    add_path(joinrel, (Path *) joinpath);
    
    /* Consider pathkeys for the join relation */
    add_paths_with_pathkeys_for_rel(root, joinrel, epq_path);
    
    /* XXX Consider parameterized paths for the join relation */
    
    // Consider rewritten MV paths to fulfil the join...
    //add_rewritten_mv_paths (root, NULL, innerrel, outerrel, joinrel, fpinfo, NULL);
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to PgFdwRelationInfo of the input relation.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel)
{
    Query	   *query = root->parse;
    PathTarget *grouping_target;
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) grouped_rel->fdw_private;
    PgFdwRelationInfo *ofpinfo;
    List	   *aggvars;
    ListCell   *lc;
    int			i;
    List	   *tlist = NIL;
    
    //elog(INFO, "%s: checking for grouping sets...", __func__);
    
    /* Grouping Sets are not pushable */
    if (query->groupingSets)
        return false;
    
    /* Get the fpinfo of the underlying scan relation. */
    ofpinfo = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;
    
    //elog(INFO, "%s: checking for local conditions...", __func__);
    
    //elog(INFO, "%s: local conditions: %s", __func__, nodeToString (ofpinfo->local_conds));
    
    /*
     * If underneath input relation has any local conditions, those conditions
     * are required to be applied before performing aggregation.  Hence the
     * aggregate cannot be pushed down.
     */
    if (ofpinfo->local_conds)
        return false;
    
    /*
     * The targetlist expected from this node and the targetlist pushed down
     * to the foreign server may be different. The latter requires
     * sortgrouprefs to be set to push down GROUP BY clause, but should not
     * have those arising from ORDER BY clause. These sortgrouprefs may be
     * different from those in the plan's targetlist. Use a copy of path
     * target to record the new sortgrouprefs.
     */
    grouping_target = copy_pathtarget(root->upper_targets[UPPERREL_GROUP_AGG]);
    
    /*
     * Evaluate grouping targets and check whether they are safe to push down
     * to the foreign side.  All GROUP BY expressions will be part of the
     * grouping target and thus there is no need to evaluate it separately.
     * While doing so, add required expressions into target list which can
     * then be used to pass to foreign server.
     */
    i = 0;
    foreach(lc, grouping_target->exprs)
    {
        Expr	   *expr = (Expr *) lfirst(lc);
        Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
        ListCell   *l;
        
        //elog(INFO, "%s: checking target expression: %s", __func__, nodeToString (expr));
        
        /* Check whether this expression is part of GROUP BY clause */
        if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
        {
            /*
             * If any of the GROUP BY expression is not shippable we can not
             * push down aggregation to the foreign server.
             */
            //elog(INFO, "%s: checking if clause is shippable...", __func__);
            
            if (!is_foreign_expr(root, grouped_rel, expr))
                return false;
            
            /* Pushable, add to tlist */
            tlist = add_to_flat_tlist(tlist, list_make1(expr));
        }
        else
        {
            //elog(INFO, "%s: checking if expression is pushable...", __func__);
            
            /* Check entire expression whether it is pushable or not */
            if (is_foreign_expr(root, grouped_rel, expr))
            {
                /* Pushable, add to tlist */
                tlist = add_to_flat_tlist(tlist, list_make1(expr));
            }
            else
            {
                /*
                 * If we have sortgroupref set, then it means that we have an
                 * ORDER BY entry pointing to this expression.  Since we are
                 * not pushing ORDER BY with GROUP BY, clear it.
                 */
                if (sgref)
                    grouping_target->sortgrouprefs[i] = 0;
                
                /* Not matched exactly, pull the var with aggregates then */
                aggvars = pull_var_clause((Node *) expr,
                                          PVC_INCLUDE_AGGREGATES);
                
                if (!is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
                    return false;
                
                /*
                 * Add aggregates, if any, into the targetlist.  Plain var
                 * nodes should be either same as some GROUP BY expression or
                 * part of some GROUP BY expression. In later case, the query
                 * cannot refer plain var nodes without the surrounding
                 * expression.  In both the cases, they are already part of
                 * the targetlist and thus no need to add them again.  In fact
                 * adding pulled plain var nodes in SELECT clause will cause
                 * an error on the foreign server if they are not same as some
                 * GROUP BY expression.
                 */
                foreach(l, aggvars)
                {
                    Expr	   *expr = (Expr *) lfirst(l);
                    
                    if (IsA(expr, Aggref))
                        tlist = add_to_flat_tlist(tlist, list_make1(expr));
                }
            }
        }
        
        i++;
    }
    
    /*
     * Classify the pushable and non-pushable having clauses and save them in
     * remote_conds and local_conds of the grouped rel's fpinfo.
     */
    if (root->hasHavingQual && query->havingQual)
    {
        ListCell   *lc;
        
        foreach(lc, (List *) query->havingQual)
        {
            Expr	   *expr = (Expr *) lfirst(lc);
            RestrictInfo *rinfo;
            
            /*
             * Currently, the core code doesn't wrap havingQuals in
             * RestrictInfos, so we must make our own.
             */
            Assert(!IsA(expr, RestrictInfo));
            rinfo = make_restrictinfo(expr,
                                      true,
                                      false,
                                      false,
                                      root->qual_security_level,
                                      grouped_rel->relids,
                                      NULL,
                                      NULL);
            if (is_foreign_expr(root, grouped_rel, expr))
                fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
            else
                fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
        }
    }
    
    /*
     * If there are any local conditions, pull Vars and aggregates from it and
     * check whether they are safe to pushdown or not.
     */
    if (fpinfo->local_conds)
    {
        List	   *aggvars = NIL;
        ListCell   *lc;
        
        foreach(lc, fpinfo->local_conds)
        {
            RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
            
            aggvars = list_concat(aggvars,
                                  pull_var_clause((Node *) rinfo->clause,
                                                  PVC_INCLUDE_AGGREGATES));
        }
        
        foreach(lc, aggvars)
        {
            Expr	   *expr = (Expr *) lfirst(lc);
            
            /*
             * If aggregates within local conditions are not safe to push
             * down, then we cannot push down the query.  Vars are already
             * part of GROUP BY clause which are checked above, so no need to
             * access them again here.
             */
            if (IsA(expr, Aggref))
            {
                //elog(INFO, "%s: checking aggregate local condition is safe to push: %s", __func__, nodeToString (expr));
                
                if (!is_foreign_expr(root, grouped_rel, expr))
                    return false;
                
                tlist = add_to_flat_tlist(tlist, list_make1(expr));
            }
        }
    }
    
    /* Transfer any sortgroupref data to the replacement tlist */
    apply_pathtarget_labeling_to_tlist(tlist, grouping_target);
    
    /* Store generated targetlist */
    fpinfo->grouped_tlist = tlist;
    
    /* Safe to pushdown */
    fpinfo->pushdown_safe = true;
    
    /*
     * Set cached relation costs to some negative value, so that we can detect
     * when they are set to some sensible costs, during one (usually the
     * first) of the calls to estimate_path_cost_size().
     */
    fpinfo->rel_startup_cost = -1;
    fpinfo->rel_total_cost = -1;
    
    /*
     * Set the string describing this grouped relation to be used in EXPLAIN
     * output of corresponding ForeignScan.
     */
    fpinfo->relation_name = makeStringInfo();
    appendStringInfo(fpinfo->relation_name, "Aggregate on (%s)",
                     ofpinfo->relation_name->data);
    
    return true;
}

/*
 * postgresGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 *
 * Right now, we only support aggregate, grouping and having clause pushdown.
 */
static void
postgresGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
                             RelOptInfo *input_rel, RelOptInfo *output_rel)
{
    PgFdwRelationInfo *fpinfo;
    
    elog(INFO, "%s (root=%p, stage=%d, input_rel=%p, output_rel=%p)", __func__, root, stage, input_rel, output_rel);
    
    /*
     * If input rel is not safe to pushdown, then simply return as we cannot
     * perform any post-join operations on the foreign server.
     */
    if (!input_rel->fdw_private ||
        !((PgFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
        return;
    
    /* Ignore stages we don't support; and skip any duplicate calls. */
    if (stage != UPPERREL_GROUP_AGG || output_rel->fdw_private)
        return;
    
    fpinfo = (PgFdwRelationInfo *) palloc0(sizeof(PgFdwRelationInfo));
    fpinfo->pushdown_safe = false;
    output_rel->fdw_private = fpinfo;
    
    add_foreign_grouping_paths(root, input_rel, output_rel);
}

static void deparse_statement_for_mv_search (StringInfo /*OUT*/ rel_sql,
                                             List /*OUT*/ **retrieved_attrs,
                                             PlannerInfo *root,
                                             RelOptInfo *grouped_rel)
{
    PgFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
    List	   *remote_exprs = NIL;
    List	   *fdw_scan_tlist = NIL;
    ForeignPath * grouppath;
    
    /* Create and add foreign path to the grouping relation. */
    grouppath = create_foreignscan_path(root,
                                        grouped_rel,
                                        root->upper_targets[UPPERREL_GROUP_AGG],
                                        1 /*rows*/, 1.0/*startup_cost*/, 2.0/*total_cost*/,
                                        NIL,	/* no pathkeys */
                                        NULL,	/* no required_outer */
                                        NULL,
                                        NIL);	/* no fdw_private */
    
    /* Build the list of columns to be fetched from the foreign server. */
    if (IS_JOIN_REL(grouped_rel) || IS_UPPER_REL(grouped_rel))
        fdw_scan_tlist = build_tlist_to_deparse(grouped_rel);
    else
        fdw_scan_tlist = NIL;
    
    remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
    
    fdw_scan_tlist = build_tlist_to_deparse(grouped_rel);
    
    deparseSelectStmtForRel(rel_sql, root, grouped_rel, fdw_scan_tlist,
                            remote_exprs, grouppath->path.pathkeys,
                            false, retrieved_attrs, NULL);
    
    pfree (grouppath);
    // FIXME: what about freeing remote_exprs?
    // FIXME: what about freeing fdw_scan_tlist?
}

Bitmapset *
recurse_relation_relids_from_rte (PlannerInfo *root, RangeTblEntry *rte, Bitmapset *relids);

Bitmapset *
recurse_relation_relids (PlannerInfo *root, Bitmapset *initial_relids, Bitmapset *out_relids)
{
    for (int x = bms_next_member (initial_relids, -1); x >= 0; x = bms_next_member (initial_relids, x))
    {
        RangeTblEntry *rte = planner_rt_fetch(x, root);
        //elog(INFO, "%s: recurse relid %d (rtekind %d)", __func__, x, rte->rtekind);
        out_relids = recurse_relation_relids_from_rte (root, rte, out_relids);
    }
    
    return out_relids;
}

Bitmapset *
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
                    out_relids = recurse_relation_relids_from_rte (root, srte, out_relids);
                }
            }
    }
    return out_relids;
}

static void
find_related_matviews_for_relation (PlannerInfo *root,
                                    RelOptInfo *input_rel, // if grouping
                                    RelOptInfo *inner_rel, RelOptInfo *outer_rel, // if joining
                                    List /* StringInfo */ **mvs_schema,
                                    List /* StringInfo */ **mvs_name,
                                    List /* StringInfo */ **mvs_definition)
{
    UserMapping *mapping;
    PGconn	   *conn;
    
    *mvs_schema = NIL, *mvs_name = NIL, *mvs_definition = NIL;
    
    Bitmapset *relids = NULL;
    if (input_rel != NULL)
        relids = recurse_relation_relids (root, input_rel->relids, NULL);
    else if (inner_rel != NULL && outer_rel != NULL)
    {
        relids = recurse_relation_relids (root, inner_rel->relids, NULL);
        relids = recurse_relation_relids (root, outer_rel->relids, relids);
    }
    
    //elog(INFO, "%s: relids=%s", __func__, bmsToString (relids));
    
    mapping = GetUserMapping(GetUserId(), GetForeignTable((Oid) bms_next_member (relids, -1))->serverid);
    
    conn = GetConnection(mapping, false);
    
    StringInfoData find_mvs_query;
    initStringInfo (&find_mvs_query);
    
    appendStringInfoString (&find_mvs_query,
                            "SELECT v.schemaname, v.matviewname, v.definition"
                            " FROM "
                            "   public.pgx_rewritable_matviews j, pg_matviews v"
                            " WHERE "
                            " j.matviewschemaname = v.schemaname"
                            " AND j.matviewname = v.matviewname"
                            " AND j.tables @> ");
    
    appendStringInfoString (&find_mvs_query, "array[");
    int i = 0;
    for (int x = bms_next_member (relids, -1); x >= 0; x = bms_next_member (relids, x))
    {
        StringInfoData foreign_table_name;
        initStringInfo (&foreign_table_name);
        
        /*
         * Core code already has some lock on each rel being planned, so we
         * can use NoLock here.
         */
        Relation	rel = heap_open((Oid) x, NoLock);
    
        deparseRelation (&foreign_table_name, rel);
        
        heap_close(rel, NoLock);
        
        char *str = PQescapeLiteral (conn, foreign_table_name.data, (size_t) foreign_table_name.len);
        
        if (i++ > 0)
            appendStringInfoString(&find_mvs_query, ", ");

        appendStringInfoString(&find_mvs_query, str);
        
        free (str);
    }
    appendStringInfoString (&find_mvs_query, "]::text[]");
    
    //elog(INFO, "%s: relids query: %s", __func__, find_mvs_query.data);

    //elog(INFO, "%s: target (local) table: %s", __func__, foreign_table_name.data);

    PGresult   *volatile res = NULL;
    
    PG_TRY();
    {
        
        res = pgfdw_exec_query_params(conn, find_mvs_query.data,
                                      0, NULL/*paramTypes*/,
                                      NULL /*paramValues*/, NULL /*paramLengths*/,
                                      NULL /*paramFormats*/,
                                      0 /*resultFormat=text*/);
        
        // FIXME: is this the best way to deap with the error?
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
            pgfdw_report_error(ERROR, res, conn, false, find_mvs_query.data);
        
        int num_mvs = PQntuples(res);
        
        for (int i = 0; i < num_mvs; i++)
        {
            //elog(INFO, "add_foreign_grouping_paths: mv[%d]={%s,%s,%s}", i,
            //     PQgetvalue(res, i, 0), PQgetvalue(res, i, 1), PQgetvalue(res, i, 2));
            
            StringInfo schema = makeStringInfo();
            appendStringInfoString (schema, PQgetvalue(res, i, 0));
            StringInfo name = makeStringInfo();
            appendStringInfoString (name, PQgetvalue(res, i, 1));
            StringInfo definition = makeStringInfo();
            appendStringInfoString (definition, PQgetvalue(res, i, 2));
            
            *mvs_schema = lappend (*mvs_schema, schema);
            *mvs_name = lappend (*mvs_name, name);
            *mvs_definition = lappend (*mvs_definition, definition);
        }
        PQclear(res);
        ReleaseConnection(conn);
    }
    PG_CATCH();
    {
        if (res)
            PQclear(res);
        ReleaseConnection(conn);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static Query *
parse_select_query (RelOptInfo *grouped_rel, const char *mv_sql)
{
    PgFdwRelationInfo *fpinfo = grouped_rel->fdw_private;

    RawStmt *mv_parsetree = list_nth_node (RawStmt, pg_parse_query (mv_sql), 0);
    
    if (fpinfo->trace_parse_select_query)
        elog(INFO, "%s: parse tree: %s", __func__, nodeToString (mv_parsetree));
    
    List *qtList = pg_analyze_and_rewrite (mv_parsetree, mv_sql, NULL, 0, NULL);
    
    Query *query = list_nth_node (Query, qtList, 0);
    
    ListCell *e;
    foreach (e, query->targetList)
    {
        TargetEntry *tle = lfirst_node (TargetEntry, e);
        
        if (fpinfo->trace_parse_select_query)
            elog(INFO, "%s: simplifying: %s", __func__, nodeToString (tle));
        
        Expr *new_expr = (Expr *) eval_const_expressions (NULL, (Node *) tle->expr);
        
        tle->expr = new_expr;

        if (fpinfo->trace_parse_select_query)
            elog(INFO, "%s: revised: %s", __func__, nodeToString (tle));
    }
    
    if (fpinfo->trace_parse_select_query)
        elog(INFO, "%s: query: %s", __func__, nodeToString (query));

    return query;
}

struct transform_todo {
    List /* Var * */ *replacement_var;
    List /* Expr * */ *replaced_expr;
};

static void
transform_todo_list_add_match (struct transform_todo *todo_list, Index i, Expr *replaced_node, TargetEntry *replacement_te)
{
    //elog(INFO, "%s: will replace node %s: with Var (varattrno=%d)", __func__, nodeToString (replaced_node), i);
    
    todo_list->replacement_var = lappend_int(todo_list->replacement_var, (int) i);
    todo_list->replaced_expr = lappend(todo_list->replaced_expr, replaced_node);
}

static Node *
transform_todos_mutator (Node *node, struct transform_todo *todo_list)
{
    ListCell *lc1, *lc2;
    forboth(lc1, todo_list->replaced_expr, lc2, todo_list->replacement_var)
    {
        Expr *replaced_node = (Expr *) lfirst (lc1);
        Index i = (Index) lfirst_int (lc2);
        
        if (replaced_node == (Expr *) node)
        {
            
            Var *v = makeNode (Var);
            v->varno = v->varnoold = REWRITTEN_VAR;
            v->varattno = v->varoattno = (int) i;
            v->varcollid = InvalidOid;
            v->varlevelsup = 0;
            v->vartype = 0; // FIXME? Not sure if this is relevant as far as deparsing goes...
            v->vartypmod = 0;
            v->location = -1;
            
            //elog(INFO, "%s: transforming node: %s into: %s", __func__, nodeToString (replaced_node), nodeToString (v));
            
            return (Node *) v;
        }
    }
    return expression_tree_mutator (node, transform_todos_mutator, todo_list);
}

List /* TargetEntry* */ *
transform_todos (List /* TargetEntry* */ *tlist, struct transform_todo *todo_list)
{
    return (List *) expression_tree_mutator ((Node *) tlist, transform_todos_mutator, todo_list);
}

static bool
check_expr_targets_in_matview_tlist (PlannerInfo *root,
                                     Query *parsed_mv_query,
                                     Expr *expr,
                                     struct transform_todo *todo_list,
                                     bool trace);

struct expr_targets_equals_ctx
{
    PlannerInfo *root;
    List *parsed_mv_query_rtable; // parsed_mv_query->rtable;
};

static bool
expr_targets_equals_walker (Node *a, Node *b, struct expr_targets_equals_ctx *ctx)
{
    //elog (INFO, "%s: comparing: %s with: %s", __func__, nodeToString (a), nodeToString (b));
    
    if (a != NULL && b != NULL && nodeTag(a) == nodeTag(b) && nodeTag(a) == T_Var)
    {
        //elog (INFO, "%s: comparing Var", __func__);
        
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
    
    bool result = equal_tree_walker(a, b, expr_targets_equals_walker, ctx);
    
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

struct expr_targets_in_matview_tlist_ctx
{
    PlannerInfo *root;
    List /* TargetEntry* */ *tList;
    bool trace; // enable trace logging
    bool match_found;
    bool did_search_anything;
    struct transform_todo *todo_list;
    struct expr_targets_equals_ctx *col_names;
};

static bool
expr_targets_in_matview_tlist_walker (Node *node, struct expr_targets_in_matview_tlist_ctx *ctx)
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
            
            local_match_found = expr_targets_equals_walker ((Node *) node, (Node *) te->expr, ctx->col_names);
            
            if (local_match_found)
            {
                if (ctx->trace)
                    elog(INFO, "%s: matched!", __func__);
                
                transform_todo_list_add_match (ctx->todo_list, i, (Expr *) node, te);
                
                done = true;
                
                break;
            }
            i++;
        }
    }
    
    if (!done)
    {
        // Copy current context
        struct expr_targets_in_matview_tlist_ctx sub_ctx = *ctx;
        
        sub_ctx.match_found = true; // walker will set to false if not matched
        sub_ctx.did_search_anything = false; // indicates whether match_found is valid
        
        if (ctx->trace)
            elog(INFO, "%s: walking sub-expressions...", __func__);
        
        // Look at the sub-expressions inside the Node. All must either match
        // exactly, or derive from something that does.
        expression_tree_walker (node, expr_targets_in_matview_tlist_walker, &sub_ctx);
        
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
check_expr_targets_in_matview_tlist (PlannerInfo *root,
                                     Query *parsed_mv_query,
                                     Expr *expr,
                                     struct transform_todo *todo_list,
                                     bool trace)
{
    if (expr == NULL)
        return true;
    
    struct expr_targets_equals_ctx col_names = {
        root, parsed_mv_query->rtable
    };
    struct expr_targets_in_matview_tlist_ctx ctx = {
        root, parsed_mv_query->targetList, trace
    };
    ctx.match_found = true; // walker will set to false if not matched
    ctx.todo_list = todo_list;
    ctx.col_names = &col_names;
    
    expr_targets_in_matview_tlist_walker ((Node *) expr, &ctx);
    
    return ctx.match_found;
}

static List /* Value* */ *
matview_tlist_colnames (Query *mv_query)
{
    List *colnames = NIL;
    
    ListCell *mv_lc;
    foreach (mv_lc, mv_query->targetList)
    {
        TargetEntry *mv_tle = lfirst_node (TargetEntry, mv_lc);
        
        colnames = lappend (colnames, makeString (mv_tle->resname));
    }
    
    return colnames;
}

static bool
check_group_clauses_for_matview (PlannerInfo *root,
                                 Query *parsed_mv_query,
                                 RelOptInfo *grouped_rel,
                                 List *transformed_tlist,
                                 struct transform_todo *todo_list)
{
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) grouped_rel->fdw_private;

    //elog(INFO, "%s: target list: %s", __func__, nodeToString(transformed_tlist));
    
    //elog(INFO, "%s: in_colnames: %s", __func__, nodeToString(in_colnames));
    //elog(INFO, "%s: mv_from_colnames: %s", __func__, nodeToString(mv_from_colnames));
    
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
        if (!check_expr_targets_in_matview_tlist (root, parsed_mv_query, expr, todo_list, fpinfo->trace_group_clause_source_check))
        {
            elog(INFO, "%s: GROUP BY clause (%s) not found in MV SELECT list", __func__, nodeToString(expr));
            return false;
        }
    }

    return true;
}

static bool
check_from_join_clauses_for_matview (PlannerInfo *root,
                                     Query *parsed_mv_query,
                                     RelOptInfo *grouped_rel,
                                     List **additional_where_clauses,
                                     struct transform_todo *todo_list)
{
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) grouped_rel->fdw_private;
    
    if (fpinfo->trace_join_clause_check)
        elog(INFO, "%s: grouped_rel: %s", __func__, nodeToString (grouped_rel));

    // 1. We don't need to check the rels involved — they must be joined
    //    by the MV otherwise they wouldn't have been selected for evaluation.
        
    // 2. However, we do need to check the type of JOIN (INNER, OUTER, etc.)
    // FIXME!

    // 3. Go through each JOIN clause in the MV.

    if (fpinfo->debug_join_clause_check)
        elog(INFO, "%s: MV jointree: %s", __func__, nodeToString (parsed_mv_query->jointree));

    List *quals = NIL; // additional qualifiers over and above the join clauses
   
    // Aggregate all the join expressions and qualifiers
    if (parsed_mv_query->jointree != NULL)
    {
        if (parsed_mv_query->jointree->quals != NULL)
        {
            if (IsA (parsed_mv_query->jointree->quals, BoolExpr) &&
                ((BoolExpr *) (parsed_mv_query->jointree->quals))->boolop == AND_EXPR)
            {
                quals = list_concat (quals, ((BoolExpr *) (parsed_mv_query->jointree->quals))->args);
            }
            else
            {
                quals = lappend (quals, (Node *) parsed_mv_query->jointree->quals);
            }
        }
        
        if (parsed_mv_query->jointree->fromlist != NULL)
        {
            ListCell   *lc1;
            foreach (lc1, parsed_mv_query->jointree->fromlist)
            {
                Node *n = lfirst (lc1);
                if (IsA (n, JoinExpr))
                {
                    JoinExpr *je = (JoinExpr *) n;
                
                    if (je->quals != NULL)
                    {
                        if (IsA (((JoinExpr *)je)->quals, BoolExpr))
                            if (((BoolExpr *) ((JoinExpr *)je)->quals)->boolop == AND_EXPR)
                                quals = list_concat (quals, ((BoolExpr *) ((JoinExpr *)je)->quals)->args);
                            else
                                quals = lappend (quals, ((JoinExpr *)je)->quals);
                        else
                            quals = lappend (quals, ((JoinExpr *)je)->quals);
                    }
                }
            }
        }
    }
    
    if (fpinfo->debug_join_clause_check)
        elog(INFO, "%s: aggregated MV join clauses: %s", __func__, nodeToString (quals));
    
    // 3.1. We must find all its expressions present in our grouped_rel query
    //      otherwise the MV is potentially too restrictive.

    List /* RestrictInfo* */ *grouped_rel_clauses;
    
    // For upper relations, the WHERE clause is built from the remote
    // conditions of the underlying scan relation.
    {
        PgFdwRelationInfo *ofpinfo;
        
        ofpinfo = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;
        grouped_rel_clauses = list_copy (ofpinfo->remote_conds);
    }
    
    List /* RestrictInfo* */ *found_grouped_rel_clauses = NIL;

    if (fpinfo->debug_join_clause_check)
        elog(INFO, "%s: grouped_rel clauses: %s", __func__, nodeToString (grouped_rel_clauses));
    
    struct expr_targets_equals_ctx col_names = {
        root, parsed_mv_query->rtable
    };

    ListCell *lc2;
    foreach (lc2, quals)
    {
        Expr *je = lfirst (lc2);

        if (fpinfo->debug_join_clause_check)
            elog(INFO, "%s: evaluating JOIN expression: %s", __func__, nodeToString (je));

        bool found = false;
        
        ListCell *lc3;
        foreach (lc3, grouped_rel_clauses)
        {
            RestrictInfo *ri = lfirst (lc3);
            
            if (fpinfo->trace_join_clause_check)
                elog(INFO, "%s: against expression: %s", __func__, deparseNode ((Node *) ri, root, grouped_rel));
            if (fpinfo->trace_join_clause_check)
                elog(INFO, "%s: against expression: %s", __func__, nodeToString (ri));

            if (expr_targets_equals_walker ((Node *) ri->clause, (Node *) je, &col_names))
            {
                if (fpinfo->debug_join_clause_check)
                    elog(INFO, "%s: matched expression: %s", __func__, deparseNode ((Node *) ri, root, grouped_rel));
                if (fpinfo->debug_join_clause_check)
                    elog(INFO, "%s: matched expression: %s", __func__, nodeToString (ri));

                found_grouped_rel_clauses = lappend (found_grouped_rel_clauses, ri);
                grouped_rel_clauses = list_delete (grouped_rel_clauses, ri);
                found = true;
                break;
            }
        }
        
        if (!found)
        {
            elog(INFO, "%s: expression not found in grouped rel: %s", __func__, nodeToString (je));
            return false;
        }
    }

    //elog(INFO, "%s: all MV JOIN expressions found in grouped rel", __func__);
    
    // 4. The balance of clauses must now be treated as if they were part of the
    //    WHERE clause list. Return them so the WHERE clause processing can handle
    //    them appropriately.
    
    *additional_where_clauses = grouped_rel_clauses;

    if (fpinfo->debug_join_clause_check)
        elog(INFO, "%s: balance of clauses: %s", __func__, deparseNode ((Node *) *additional_where_clauses, root, grouped_rel));
    if (fpinfo->debug_join_clause_check)
        elog(INFO, "%s: balance of clauses: %s", __func__, nodeToString (*additional_where_clauses));
    
    return true;
}

static bool
check_where_clauses_source_from_matview_tlist (PlannerInfo *root,
                                               Query *parsed_mv_query,
                                               RelOptInfo *grouped_rel,
                                               List *additional_clauses,
                                               List **transformed_clist_p,
                                               struct transform_todo *todo_list)
{
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) grouped_rel->fdw_private;

    if (fpinfo->trace_where_clause_source_check)
        elog(INFO, "%s: clauses: %s", __func__, nodeToString (((PgFdwRelationInfo *)fpinfo->outerrel->fdw_private)->remote_conds));
    
    ListCell   *lc;
    foreach (lc, list_concat (((PgFdwRelationInfo *)fpinfo->outerrel->fdw_private)->remote_conds,
                              additional_clauses))
    {
        Expr *expr = lfirst (lc);
        
        /* Extract clause from RestrictInfo, if required */
        if (IsA(expr, RestrictInfo))
        {
            expr = ((RestrictInfo *) expr)->clause;
        }
        
        if (!check_expr_targets_in_matview_tlist (root, parsed_mv_query, expr, todo_list, fpinfo->trace_where_clause_source_check))
        {
            elog(INFO, "%s: WHERE clause (%s) not found in MV SELECT list", __func__, nodeToString(expr));
            return false;
        }
        
        *transformed_clist_p = lappend (*transformed_clist_p, expr);
    }
    
    return true;
}

static bool
check_select_clauses_source_from_matview_tlist (PlannerInfo *root,
                                                Query *parsed_mv_query,
                                                RelOptInfo *grouped_rel,
                                                List /* TargetEntry* */ *selected_tlist,
                                                struct transform_todo *todo_list)
{
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) grouped_rel->fdw_private;

    ListCell   *lc;
    foreach (lc, selected_tlist)
    {
        Expr *expr = lfirst(lc);
        
        //elog(INFO, "%s: matching expr: %s", __func__, nodeToString (expr));
        
        if (!check_expr_targets_in_matview_tlist (root, parsed_mv_query, expr, todo_list, fpinfo->trace_select_clause_source_check))
        {
            elog(INFO, "%s: expr (%s) not found in MV tlist", __func__, nodeToString (expr));
            return false;
        }
    }
    
    // Complete match found!
    return true;
}

static void
estimate_query_cost (PlannerInfo *root,
                     RelOptInfo *grouped_rel,
                     const char *query,
                     double *rows, int *width, Cost *startup_cost, Cost *total_cost)
{
    PgFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
    
    StringInfoData sql;
    initStringInfo(&sql);
    appendStringInfoString(&sql, "EXPLAIN ");
    appendStringInfoString(&sql, query);
    
    //elog(INFO, "%s: explain SQL: %s", __func__, query);
    
    PGconn	   *conn = GetConnection(fpinfo->user, false);
    get_remote_estimate(sql.data, conn, rows, width, startup_cost, total_cost);
    ReleaseConnection(conn);
}

static bool
evaluate_matview_for_rewrite (PlannerInfo *root,
                              RelOptInfo *grouped_rel,
                              List *grouped_tlist,
                              StringInfo mv_name, StringInfo mv_schema, StringInfo mv_definition,
                              StringInfo *alternative_query)
{
    // First, copy some structures from the grouped_rel. We'll begin the actual
    // transformation later.
    List /* Expr* */ *transformed_clist = NIL;
    List /* TargetEntry * */ *transformed_tlist = list_copy (grouped_tlist);
    
    struct transform_todo transform_todo_list = { NULL, NIL };
    
    //elog(INFO, "%s: evaluating MV: %s.%s", __func__, mv_schema->data, mv_name->data);
    
    Query *parsed_mv_query = parse_select_query (grouped_rel, (const char *) mv_definition->data);
    
    // 1. Check the GROUP BY clause: it must match exactly
    //elog(INFO, "%s: checking GROUP BY clauses...", __func__);
    
    if (!check_group_clauses_for_matview(root, parsed_mv_query, grouped_rel, transformed_tlist, &transform_todo_list))
        return false;
    
    // FIXME: the above check only checks some selected clauses; the balance of
    // non-GROUPed columns would need to be re-aggregated by the outer, hence the above
    // check needs to be exact set equality.
    
    // FIXME: 1a. Allow a GROUP BY superset and push a re-group to outer
    // where it can be re-aggregated
    
    List *additional_where_clauses = NIL;
    
    // 2. Check the FROM and WHERE clauses: they must match exactly
    //elog(INFO, "%s: checking FROM clauses...", __func__);
    if (!check_from_join_clauses_for_matview (root, parsed_mv_query, grouped_rel, &additional_where_clauses,
                                              &transform_todo_list))
        return false;
    
    // 3a. Allow more WHERE clauses only where they source from MV tlist cols
    
    if (!check_where_clauses_source_from_matview_tlist(root, parsed_mv_query, grouped_rel, additional_where_clauses,
                                                       &transformed_clist, &transform_todo_list))
        return false;
    
    // 4. Check for HAVING clause: push them in to the WHERE list
    //elog(INFO, "%s: checking HAVING clauses...", __func__);
    
    // FIXME: 5. Consider computing any missing aggregates from other components
    
    // 6. Check the SELECT clauses: they must be a subset
    //elog(INFO, "%s: checking SELECT clauses...", __func__);
    
    if (!check_select_clauses_source_from_matview_tlist (root, parsed_mv_query, grouped_rel, grouped_tlist, &transform_todo_list))
        return false;
    
    // FIXME: 6a. Allow SELECT of an expression based on the fundamental
    // MV's select tList. This may necessitate re-computing the expression list
    // to contain direct ColumnRefs instead of Exprs where those are matched.
    // In turn, this reuires recursive processing of the query tree to search
    // for matches. After that, the whole matter becomes a regular check for
    // push-down-ability.
    
    // FIXME: 7. Create a new grouped_rel and transform it
    // FIXME: 7.1: Copy the grouped_rel
    // FIXME: 7.2: Transform the grouped_rel.relid to target the MV
    // FIXME: 7.2: Fix-up the RTE to match the MV
    // FIXME: 7.3: Transform exprs that match exprs in the MV into Vars
    transformed_tlist = transform_todos (transformed_tlist, &transform_todo_list);
    //elog(INFO, "%s: transformed target list: %s", __func__, nodeToString (transformed_tlist));
    
    // FIXME: 7.3: Fix-up the other remaining Var pointers
    // FIXME: 7.4: Remove the residual GROUP BY clause
    // FIXME: 7.5: Transform any HAVING clauses into WHERE clauses.
    
    // Build the alternative query.
    
    *alternative_query = makeStringInfo();
    {
        /* Fill portions of context common to upper, join and base relation */
        deparse_expr_cxt context;
        context.buf = *alternative_query;
        context.root = root;
        context.foreignrel = grouped_rel;
        context.scanrel = grouped_rel;
        context.params_list = NULL;
        context.colnames = matview_tlist_colnames (parsed_mv_query);
        
        appendStringInfoString (*alternative_query, "SELECT ");
        // FIXME: this seems not to work; make a more simplistic deparser to pull directly from the parsed MV query.
        
        deparseExplicitTargetList (transformed_tlist, NULL, &context);
        
        appendStringInfo (*alternative_query, " FROM %s.%s", mv_schema->data, mv_name->data);
        
        List *quals = transform_todos (transformed_clist, &transform_todo_list);
        
        // FIXME: this seems not to append the JOIN info...
        
        //elog(INFO, "%s: transformed quals list: %s", __func__, nodeToString (quals));
        
        if (quals != NIL)
        {
            appendStringInfo (*alternative_query, " WHERE ");
            appendConditions (quals, &context);
        }
        // FIXME: do we need to append the ORDER BY clause too?
    }

    return true;
}

void add_rewritten_mv_paths (PlannerInfo *root,
                             RelOptInfo *input_rel, // if grouping
                             RelOptInfo *inner_rel, RelOptInfo *outer_rel, // if joining
                             RelOptInfo *grouped_rel,
                             PgFdwRelationInfo *fpinfo, PathTarget *grouping_target)
{
    ForeignPath *grouppath;

    //elog(INFO, "%s: searching for MVs...", __func__);
    
    // See if there are any candidate MVs...
    List *mvs_schema, *mvs_name, *mvs_definition;
    
    find_related_matviews_for_relation (root, input_rel, inner_rel, outer_rel,
                                        &mvs_schema, &mvs_name, &mvs_definition);

    List	*retrieved_attrs;
    
    // Deparse:
    //   1. the input relation (i.e., the ungrouped source table)
    //   2. the grouped relation (i.e., the input relation with all the aggregates applied)
    {
        StringInfoData rel_sql;
        
        initStringInfo(&rel_sql);
        
        deparse_statement_for_mv_search (&rel_sql, &retrieved_attrs, root, grouped_rel);
        
        elog(INFO, "%s: grouped_rel: SQL: %s", __func__, rel_sql.data);
        
        if (input_rel != NULL)
        {
            initStringInfo (&rel_sql);
            
            deparse_statement_for_mv_search (&rel_sql, NULL, root, input_rel);
        
            elog(INFO, "%s: input_rel: SQL: %s", __func__, rel_sql.data);
        }
    }
    
    List /* TargetEntry * */ *grouped_tlist = build_tlist_to_deparse (grouped_rel);
    
    // Evaluate each MV in turn...
    ListCell   *sc, *nc, *dc;
    forthree (sc, mvs_schema, nc, mvs_name, dc, mvs_definition)
    {
        StringInfo mv_schema = lfirst(sc), mv_name = lfirst(nc), mv_definition = lfirst(dc);
        
        elog(INFO, "%s: evaluating MV: %s.%s", __func__, mv_schema->data, mv_name->data);

        StringInfo alternative_query;
        
        if (!evaluate_matview_for_rewrite (root, grouped_rel, grouped_tlist, mv_name, mv_schema, mv_definition, &alternative_query))
            continue;
        
        //elog(INFO, "%s: alternative query: %s", __func__, alternative_query->data);
        
        // 7. If all is well, make a cost estimate
        //elog(INFO, "%s: making cost estimate...", __func__);
        
        double		rows;
        int			width;
        Cost		startup_cost;
        Cost		total_cost;
        
        estimate_query_cost (root, grouped_rel,
                             alternative_query->data, &rows, &width, &startup_cost, &total_cost);
        
        // FIXME: we consider that there are no locally-checked quals
        // to save building all that cruft here. But in reality, that
        // might be wrong.
        
        // 8. Finally, create and add the path
        //elog(INFO, "%s: creating and adding path...", __func__);
        
        List *fdw_private = list_make3 (makeString (alternative_query->data),
                                        retrieved_attrs,
                                        makeInteger (fpinfo->fetch_size));
        fdw_private = lappend(fdw_private, makeString(fpinfo->relation_name->data));
        
        // We still add the path to the raw grouped_rel — the foreign query is
        // the one that is modified to scan the MV, and the local planner need
        // know nothing of it except for the reduced plan cost.
        grouppath = create_foreignscan_path(root,
                                            grouped_rel,
                                            grouping_target,
                                            rows,
                                            startup_cost,
                                            total_cost,
                                            NIL,	/* no pathkeys */
                                            NULL,	/* no required_outer */
                                            NULL,
                                            fdw_private);	/* no fdw_private */
        
        /* Add generated path into grouped_rel by add_path(). */
        add_path(grouped_rel, (Path *) grouppath);
    }
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
                           RelOptInfo *grouped_rel)
{
    Query	   *parse = root->parse;
    PgFdwRelationInfo *ifpinfo = input_rel->fdw_private;
    PgFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
    ForeignPath *grouppath;
    PathTarget *grouping_target;
    double		rows;
    int			width;
    Cost		startup_cost;
    Cost		total_cost;
    
    //elog(INFO, "%s", __func__);
    
    //elog(INFO, "%s: root: %s", __func__, nodeToString (root));
    //elog(INFO, "%s: input_rel: %s", __func__, nodeToString (input_rel));
    //elog(INFO, "%s: grouped_rel: %s", __func__, nodeToString (grouped_rel));
    
    /* Nothing to be done, if there is no grouping or aggregation required. */
    if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
        !root->hasHavingQual)
        return;
    
    grouping_target = root->upper_targets[UPPERREL_GROUP_AGG];
    
    /* save the input_rel as outerrel in fpinfo */
    fpinfo->outerrel = input_rel;
    
    /*
     * Copy foreign table, foreign server, user mapping, FDW options etc.
     * details from the input relation's fpinfo.
     */
    fpinfo->table = ifpinfo->table;
    fpinfo->server = ifpinfo->server;
    fpinfo->user = ifpinfo->user;
    merge_fdw_options(fpinfo, ifpinfo, NULL);
    
    //elog(INFO, "%s: assessing push-down safety...", __func__);
    
    /* Assess if it is safe to push down aggregation and grouping. */
    if (!foreign_grouping_ok(root, grouped_rel))
        return;
    
    //elog(INFO, "%s: push-down safety OK.", __func__);
    
    /* Estimate the cost of push down */
    estimate_path_cost_size(root, grouped_rel, NIL, NIL, &rows,
                            &width, &startup_cost, &total_cost);
    
    /* Now update this information in the fpinfo */
    fpinfo->rows = rows;
    fpinfo->width = width;
    fpinfo->startup_cost = startup_cost;
    fpinfo->total_cost = total_cost;
    
    //elog(INFO, "add_foreign_grouping_paths: adding path");
    
    /* Create and add foreign path to the grouping relation. */
    grouppath = create_foreignscan_path(root,
                                        grouped_rel,
                                        grouping_target,
                                        rows,
                                        startup_cost,
                                        total_cost,
                                        NIL,	/* no pathkeys */
                                        NULL,	/* no required_outer */
                                        NULL,
                                        NIL);	/* no fdw_private */
    
    /* Add generated path into grouped_rel by add_path(). */
    add_path(grouped_rel, (Path *) grouppath);
    
    add_rewritten_mv_paths(root, input_rel, NULL, NULL, grouped_rel, fpinfo, grouping_target);
}

/*
 * Create a tuple from the specified row of the PGresult.
 *
 * rel is the local representation of the foreign table, attinmeta is
 * conversion data for the rel's tupdesc, and retrieved_attrs is an
 * integer list of the table column numbers present in the PGresult.
 * temp_context is a working context that can be reset after each tuple.
 */
static HeapTuple
make_tuple_from_result_row(PGresult *res,
                           int row,
                           Relation rel,
                           AttInMetadata *attinmeta,
                           List *retrieved_attrs,
                           ForeignScanState *fsstate,
                           MemoryContext temp_context)
{
    HeapTuple	tuple;
    TupleDesc	tupdesc;
    Datum	   *values;
    bool	   *nulls;
    ItemPointer ctid = NULL;
    Oid			oid = InvalidOid;
    ConversionLocation errpos;
    ErrorContextCallback errcallback;
    MemoryContext oldcontext;
    ListCell   *lc;
    int			j;
    
    Assert(row < PQntuples(res));
    
    /*
     * Do the following work in a temp context that we reset after each tuple.
     * This cleans up not only the data we have direct access to, but any
     * cruft the I/O functions might leak.
     */
    oldcontext = MemoryContextSwitchTo(temp_context);
    
    if (rel)
        tupdesc = RelationGetDescr(rel);
    else
    {
        PgFdwScanState *fdw_sstate;
        
        Assert(fsstate);
        fdw_sstate = (PgFdwScanState *) fsstate->fdw_state;
        tupdesc = fdw_sstate->tupdesc;
    }
    
    values = (Datum *) palloc0((size_t) tupdesc->natts * sizeof(Datum));
    nulls = (bool *) palloc((size_t) tupdesc->natts * sizeof(bool));
    /* Initialize to nulls for any columns not present in result */
    memset(nulls, true, (size_t) tupdesc->natts * sizeof(bool));
    
    /*
     * Set up and install callback to report where conversion error occurs.
     */
    errpos.rel = rel;
    errpos.cur_attno = 0;
    errpos.fsstate = fsstate;
    errcallback.callback = conversion_error_callback;
    errcallback.arg = (void *) &errpos;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;
    
    /*
     * i indexes columns in the relation, j indexes columns in the PGresult.
     */
    j = 0;
    foreach(lc, retrieved_attrs)
    {
        int			i = lfirst_int(lc);
        char	   *valstr;
        
        /* fetch next column's textual value */
        if (PQgetisnull(res, row, j))
            valstr = NULL;
        else
            valstr = PQgetvalue(res, row, j);
        
        /*
         * convert value to internal representation
         *
         * Note: we ignore system columns other than ctid and oid in result
         */
        errpos.cur_attno = i;
        if (i > 0)
        {
            /* ordinary column */
            Assert(i <= tupdesc->natts);
            nulls[i - 1] = (valstr == NULL);
            /* Apply the input function even to nulls, to support domains */
            values[i - 1] = InputFunctionCall(&attinmeta->attinfuncs[i - 1],
                                              valstr,
                                              attinmeta->attioparams[i - 1],
                                              attinmeta->atttypmods[i - 1]);
        }
        else if (i == SelfItemPointerAttributeNumber)
        {
            /* ctid */
            if (valstr != NULL)
            {
                Datum		datum;
                
                datum = DirectFunctionCall1(tidin, CStringGetDatum(valstr));
                ctid = (ItemPointer) DatumGetPointer(datum);
            }
        }
        else if (i == ObjectIdAttributeNumber)
        {
            /* oid */
            if (valstr != NULL)
            {
                Datum		datum;
                
                datum = DirectFunctionCall1(oidin, CStringGetDatum(valstr));
                oid = DatumGetObjectId(datum);
            }
        }
        errpos.cur_attno = 0;
        
        j++;
    }
    
    /* Uninstall error context callback. */
    error_context_stack = errcallback.previous;
    
    /*
     * Check we got the expected number of columns.  Note: j == 0 and
     * PQnfields == 1 is expected, since deparse emits a NULL if no columns.
     */
    if (j > 0 && j != PQnfields(res))
        elog(ERROR, "remote query result does not match the foreign table");
    
    /*
     * Build the result tuple in caller's memory context.
     */
    MemoryContextSwitchTo(oldcontext);
    
    tuple = heap_form_tuple(tupdesc, values, nulls);
    
    /*
     * If we have a CTID to return, install it in both t_self and t_ctid.
     * t_self is the normal place, but if the tuple is converted to a
     * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
     * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
     */
    if (ctid)
        tuple->t_self = tuple->t_data->t_ctid = *ctid;
    
    /*
     * Stomp on the xmin, xmax, and cmin fields from the tuple created by
     * heap_form_tuple.  heap_form_tuple actually creates the tuple with
     * DatumTupleFields, not HeapTupleFields, but the executor expects
     * HeapTupleFields and will happily extract system columns on that
     * assumption.  If we don't do this then, for example, the tuple length
     * ends up in the xmin field, which isn't what we want.
     */
    HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
    HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
    HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);
    
    /*
     * If we have an OID to return, install it.
     */
    if (OidIsValid(oid))
        HeapTupleSetOid(tuple, oid);
    
    /* Clean up */
    MemoryContextReset(temp_context);
    
    return tuple;
}

/*
 * Callback function which is called when error occurs during column value
 * conversion.  Print names of column and relation.
 */
static void
conversion_error_callback(void *arg)
{
    const char *attname = NULL;
    const char *relname = NULL;
    bool		is_wholerow = false;
    ConversionLocation *errpos = (ConversionLocation *) arg;
    
    if (errpos->rel)
    {
        /* error occurred in a scan against a foreign table */
        TupleDesc	tupdesc = RelationGetDescr(errpos->rel);
        
        if (errpos->cur_attno > 0 && errpos->cur_attno <= tupdesc->natts)
            attname = NameStr(tupdesc->attrs[errpos->cur_attno - 1]->attname);
        else if (errpos->cur_attno == SelfItemPointerAttributeNumber)
            attname = "ctid";
        else if (errpos->cur_attno == ObjectIdAttributeNumber)
            attname = "oid";
        
        relname = RelationGetRelationName(errpos->rel);
    }
    else
    {
        /* error occurred in a scan against a foreign join */
        ForeignScanState *fsstate = errpos->fsstate;
        ForeignScan *fsplan = castNode(ForeignScan, fsstate->ss.ps.plan);
        EState	   *estate = fsstate->ss.ps.state;
        TargetEntry *tle;
        
        tle = list_nth_node(TargetEntry, fsplan->fdw_scan_tlist,
                            errpos->cur_attno - 1);
        
        /*
         * Target list can have Vars and expressions.  For Vars, we can get
         * it's relation, however for expressions we can't.  Thus for
         * expressions, just show generic context message.
         */
        if (IsA(tle->expr, Var))
        {
            RangeTblEntry *rte;
            Var		   *var = (Var *) tle->expr;
            
            rte = rt_fetch((int) var->varno, estate->es_range_table);
            
            if (var->varattno == 0)
                is_wholerow = true;
            else
                attname = get_relid_attribute_name(rte->relid, var->varattno);
            
            relname = get_rel_name(rte->relid);
        }
        else
            errcontext("processing expression at position %d in select list",
                       errpos->cur_attno);
    }
    
    if (relname)
    {
        if (is_wholerow)
            errcontext("whole-row reference to foreign table \"%s\"", relname);
        else if (attname)
            errcontext("column \"%s\" of foreign table \"%s\"", attname, relname);
    }
}

/*
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 */
extern Expr *
find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
    ListCell   *lc_em;
    
    foreach(lc_em, ec->ec_members)
    {
        EquivalenceMember *em = lfirst(lc_em);
        
        if (bms_is_subset(em->em_relids, rel->relids))
        {
            /*
             * If there is more than one equivalence member whose Vars are
             * taken entirely from this relation, we'll be content to choose
             * any one of those.
             */
            return em->em_expr;
        }
    }
    
    /* We didn't find any suitable equivalence class expression */
    return NULL;
}
