//
//  build_joinrel_restrictlist.h
//  mv_rewrite
//
//  Created by John Dent on 08/07/2018.
//  Copyright © 2018 John Dent. All rights reserved.
//

#ifndef build_joinrel_restrictlist_h
#define build_joinrel_restrictlist_h

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

extern List *
build_joinrel_restrictlist(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outer_rel,
						   RelOptInfo *inner_rel);

#endif /* build_joinrel_restrictlist_h */
