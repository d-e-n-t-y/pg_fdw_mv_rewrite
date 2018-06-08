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

#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/relcache.h"

#include "libpq-fe.h"

extern void
mv_rewrite_create_upper_paths_hook(PlannerInfo *root,
			      UpperRelationKind stage,
			      RelOptInfo *input_rel,
			      RelOptInfo *output_rel);

#endif							/* MV_REWRITE_H */
