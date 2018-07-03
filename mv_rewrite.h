/*-------------------------------------------------------------------------
 *
 * mv_rewrite.h
 *		  Query rewrite for MVs
 *
 * Portions Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef MV_REWRITE_H
#define MV_REWRITE_H

#include "pg_config.h"

#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/relcache.h"
#include "optimizer/planner.h"

#include "libpq-fe.h"

extern void
mv_rewrite_set_join_pathlist_hook (PlannerInfo *root,
								   RelOptInfo *joinrel,
								   RelOptInfo *outerrel,
								   RelOptInfo *innerrel,
								   JoinType jointype,
								   JoinPathExtraData *extra);

extern void
mv_rewrite_create_upper_paths_hook (PlannerInfo *root,
									UpperRelationKind stage,
									RelOptInfo *input_rel,
									RelOptInfo *output_rel
#if PG_VERSION_NUM >= 110000
									, void *extra
#endif
									);

#endif							/* MV_REWRITE_H */
