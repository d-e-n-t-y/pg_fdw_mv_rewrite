//
//  exension.h
//  pg_fdw_mv_rewrite
//
//  Created by John Dent on 28/04/2018.
//  Copyright © 2018 John Dent. All rights reserved.
//

#ifndef extension_h
#define extension_h

#include "optimizer/planner.h"

extern void
_PG_init (void);

extern void
_PG_fini (void);

/*
 * Old hook values.
 *
 * We will chain to these (if set) after we've done our work. It will
 * be set to the current value (if any) of the hook during initialization.
 */
extern create_upper_paths_hook_type next_create_upper_paths_hook;

#endif /* exension_h */
