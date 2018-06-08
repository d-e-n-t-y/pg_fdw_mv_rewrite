//
//  join_is_legal.h
//
//  Created by John Dent on 27/05/2018.
//  Copyright © 2018 John Dent. All rights reserved.
//

#ifndef join_is_legal_h
#define join_is_legal_h

#include <stdio.h>

bool
join_is_legal(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2,
			  Relids joinrelids,
			  SpecialJoinInfo **sjinfo_p, bool *reversed_p);

#endif /* join_is_legal_h */
