//
//  exension.c
//  pg_fdw_mv_rewrite
//
//  Created by John Dent on 28/04/2018.
//  Copyright © 2018 John Dent. All rights reserved.
//

#include "postgres.h"

#include "extension.h"
#include "postgres_fdw.h"

extern create_upper_paths_hook_type create_upper_paths_hook;

/*
 * Old hook values.
 *
 * We will chain to these (if set) after we've done our work. It will
 * be set to the current value (if any) of the hook during initialization.
 */
create_upper_paths_hook_type next_create_upper_paths_hook = NULL;

void
_PG_init (void)
{
	next_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = pg_mv_rewrite_get_upper_paths;
}

void
_PG_fini (void)
{
	create_upper_paths_hook = next_create_upper_paths_hook;
	next_create_upper_paths_hook = NULL;
}
