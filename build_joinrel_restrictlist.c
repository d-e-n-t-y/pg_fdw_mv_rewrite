//
//  build_joinrel_restrictlist.c
//
// copied from src/backend/optimizer/util/relnode.c
//

#include "build_joinrel_restrictlist.h"

#include "postgres.h"

#include <limits.h>

#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/plancat.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "utils/hsearch.h"

static List *subbuild_joinrel_restrictlist(RelOptInfo *joinrel,
										   List *joininfo_list,
										   List *new_restrictlist);

/*
 * build_joinrel_restrictlist
 * build_joinrel_joinlist
 *	  These routines build lists of restriction and join clauses for a
 *	  join relation from the joininfo lists of the relations it joins.
 *
 *	  These routines are separate because the restriction list must be
 *	  built afresh for each pair of input sub-relations we consider, whereas
 *	  the join list need only be computed once for any join RelOptInfo.
 *	  The join list is fully determined by the set of rels making up the
 *	  joinrel, so we should get the same results (up to ordering) from any
 *	  candidate pair of sub-relations.  But the restriction list is whatever
 *	  is not handled in the sub-relations, so it depends on which
 *	  sub-relations are considered.
 *
 *	  If a join clause from an input relation refers to base rels still not
 *	  present in the joinrel, then it is still a join clause for the joinrel;
 *	  we put it into the joininfo list for the joinrel.  Otherwise,
 *	  the clause is now a restrict clause for the joined relation, and we
 *	  return it to the caller of build_joinrel_restrictlist() to be stored in
 *	  join paths made from this pair of sub-relations.  (It will not need to
 *	  be considered further up the join tree.)
 *
 *	  In many case we will find the same RestrictInfos in both input
 *	  relations' joinlists, so be careful to eliminate duplicates.
 *	  Pointer equality should be a sufficient test for dups, since all
 *	  the various joinlist entries ultimately refer to RestrictInfos
 *	  pushed into them by distribute_restrictinfo_to_rels().
 *
 * 'joinrel' is a join relation node
 * 'outer_rel' and 'inner_rel' are a pair of relations that can be joined
 *		to form joinrel.
 *
 * build_joinrel_restrictlist() returns a list of relevant restrictinfos,
 * whereas build_joinrel_joinlist() stores its results in the joinrel's
 * joininfo list.  One or the other must accept each given clause!
 *
 * NB: Formerly, we made deep(!) copies of each input RestrictInfo to pass
 * up to the join relation.  I believe this is no longer necessary, because
 * RestrictInfo nodes are no longer context-dependent.  Instead, just include
 * the original nodes in the lists made for the join relation.
 */
List *
build_joinrel_restrictlist(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outer_rel,
						   RelOptInfo *inner_rel)
{
	List	   *result;
	
	/*
	 * Collect all the clauses that syntactically belong at this level,
	 * eliminating any duplicates (important since we will see many of the
	 * same clauses arriving from both input relations).
	 */
	result = subbuild_joinrel_restrictlist(joinrel, outer_rel->joininfo, NIL);
	result = subbuild_joinrel_restrictlist(joinrel, inner_rel->joininfo, result);
	
	/*
	 * Add on any clauses derived from EquivalenceClasses.  These cannot be
	 * redundant with the clauses in the joininfo lists, so don't bother
	 * checking.
	 */
	result = list_concat(result,
						 generate_join_implied_equalities(root,
														  joinrel->relids,
														  outer_rel->relids,
														  inner_rel));
	
	return result;
}

static List *
subbuild_joinrel_restrictlist(RelOptInfo *joinrel,
							  List *joininfo_list,
							  List *new_restrictlist)
{
	ListCell   *l;
	
	foreach(l, joininfo_list)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
		
		if (bms_is_subset(rinfo->required_relids, joinrel->relids))
		{
			/*
			 * This clause becomes a restriction clause for the joinrel, since
			 * it refers to no outside rels.  Add it to the list, being
			 * careful to eliminate duplicates. (Since RestrictInfo nodes in
			 * different joinlists will have been multiply-linked rather than
			 * copied, pointer equality should be a sufficient test.)
			 */
			new_restrictlist = list_append_unique_ptr(new_restrictlist, rinfo);
		}
		else
		{
			/*
			 * This clause is still a join clause at this level, so we ignore
			 * it in this routine.
			 */
		}
	}
	
	return new_restrictlist;
}
