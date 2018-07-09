//
//  release_rewrite_locks.c
//
// copied from rewriteHandler.c
//

#include "release_rewrite_locks.h"

#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rowsecurity.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

typedef struct acquireLocksOnSubLinks_context
{
	bool		for_execute;	/* AcquireRewriteLocks' forExecute param */
} acquireLocksOnSubLinks_context;

static bool acquireLocksOnSubLinks(Node *node,
								   acquireLocksOnSubLinks_context *context);

/*
 * ReleaseRewriteLocks -
 *	  Release suitable locks on all the relations mentioned in the Query.
 *
 * For more information and original code, see AcquireRewriteLocks().
 */
void
ReleaseRewriteLocks(Query *parsetree,
					bool forExecute,
					bool forUpdatePushedDown)
{
	ListCell   *l;
	int			rt_index;
	acquireLocksOnSubLinks_context context;
	
	context.for_execute = forExecute;
	
	/*
	 * First, process RTEs of the current query level.
	 */
	rt_index = 0;
	foreach(l, parsetree->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);
		Relation	rel;
		LOCKMODE	lockmode;
		List	   *newaliasvars;
		Index		curinputvarno;
		RangeTblEntry *curinputrte;
		ListCell   *ll;
		
		++rt_index;
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				
				/*
				 * Grab the appropriate lock type for the relation, and do not
				 * release it until end of transaction. This protects the
				 * rewriter and planner against schema changes mid-query.
				 *
				 * Assuming forExecute is true, this logic must match what the
				 * executor will do, else we risk lock-upgrade deadlocks.
				 */
				if (!forExecute)
					lockmode = AccessShareLock;
				else if (rt_index == parsetree->resultRelation)
					lockmode = RowExclusiveLock;
				else if (forUpdatePushedDown ||
						 get_parse_rowmark(parsetree, (Index) rt_index) != NULL)
					lockmode = RowShareLock;
				else
					lockmode = AccessShareLock;
				
				rel = heap_open(rte->relid, NoLock);
				
				/*
				 * While we have the relation open, update the RTE's relkind,
				 * just in case it changed since this rule was made.
				 */
				rte->relkind = rel->rd_rel->relkind;
				
				heap_close(rel, lockmode);
				break;
				
			case RTE_JOIN:
				
				/*
				 * Scan the join's alias var list to see if any columns have
				 * been dropped, and if so replace those Vars with null
				 * pointers.
				 *
				 * Since a join has only two inputs, we can expect to see
				 * multiple references to the same input RTE; optimize away
				 * multiple fetches.
				 */
				newaliasvars = NIL;
				curinputvarno = 0;
				curinputrte = NULL;
				foreach(ll, rte->joinaliasvars)
			{
				Var		   *aliasitem = (Var *) lfirst(ll);
				Var		   *aliasvar = aliasitem;
				
				/* Look through any implicit coercion */
				aliasvar = (Var *) strip_implicit_coercions((Node *) aliasvar);
				
				/*
				 * If the list item isn't a simple Var, then it must
				 * represent a merged column, ie a USING column, and so it
				 * couldn't possibly be dropped, since it's referenced in
				 * the join clause.  (Conceivably it could also be a null
				 * pointer already?  But that's OK too.)
				 */
				if (aliasvar && IsA(aliasvar, Var))
				{
					/*
					 * The elements of an alias list have to refer to
					 * earlier RTEs of the same rtable, because that's the
					 * order the planner builds things in.  So we already
					 * processed the referenced RTE, and so it's safe to
					 * use get_rte_attribute_is_dropped on it. (This might
					 * not hold after rewriting or planning, but it's OK
					 * to assume here.)
					 */
					Assert(aliasvar->varlevelsup == 0);
					if (aliasvar->varno != curinputvarno)
					{
						curinputvarno = aliasvar->varno;
						if (curinputvarno >= rt_index)
							elog(ERROR, "unexpected varno %d in JOIN RTE %d",
								 curinputvarno, rt_index);
						curinputrte = rt_fetch((int) curinputvarno,
											   parsetree->rtable);
					}
					if (get_rte_attribute_is_dropped(curinputrte,
													 aliasvar->varattno))
					{
						/* Replace the join alias item with a NULL */
						aliasitem = NULL;
					}
				}
				newaliasvars = lappend(newaliasvars, aliasitem);
			}
				rte->joinaliasvars = newaliasvars;
				break;
				
			case RTE_SUBQUERY:
				
				/*
				 * The subquery RTE itself is all right, but we have to
				 * recurse to process the represented subquery.
				 */
				AcquireRewriteLocks(rte->subquery,
									forExecute,
									(forUpdatePushedDown ||
									 get_parse_rowmark(parsetree, (Index) rt_index) != NULL));
				break;
				
			default:
				/* ignore other types of RTEs */
				break;
		}
	}
	
	/* Recurse into subqueries in WITH */
	foreach(l, parsetree->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(l);
		
		AcquireRewriteLocks((Query *) cte->ctequery, forExecute, false);
	}
	
	/*
	 * Recurse into sublink subqueries, too.  But we already did the ones in
	 * the rtable and cteList.
	 */
	if (parsetree->hasSubLinks)
		query_tree_walker(parsetree, acquireLocksOnSubLinks, &context,
						  QTW_IGNORE_RC_SUBQUERIES);
}

/*
 * Walker to find sublink subqueries for AcquireRewriteLocks
 */
static bool
acquireLocksOnSubLinks(Node *node, acquireLocksOnSubLinks_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, SubLink))
	{
		SubLink    *sub = (SubLink *) node;
		
		/* Do what we came for */
		AcquireRewriteLocks((Query *) sub->subselect,
							context->for_execute,
							false);
		/* Fall through to process lefthand args of SubLink */
	}
	
	/*
	 * Do NOT recurse into Query nodes, because AcquireRewriteLocks already
	 * processed subselects of subselects for us.
	 */
	return expression_tree_walker(node, acquireLocksOnSubLinks, context);
}
