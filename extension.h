//
//  exension.h
//  mv_rewrite
//
//  Created by John Dent on 28/04/2018.
//  Copyright © 2018 John Dent. All rights reserved.
//

#ifndef extension_h
#define extension_h

#include "optimizer/planner.h"
#include "optimizer/paths.h"

extern bool g_rewrite_enabled;
extern bool g_log_match_progress;
extern bool g_trace_match_progress;
extern bool g_trace_parse_select_query;
extern bool g_trace_group_clause_source_check;
extern bool g_trace_having_clause_source_check;
extern bool g_trace_where_clause_source_check;
extern bool g_trace_select_clause_source_check;
extern bool g_trace_join_clause_check;
extern bool g_debug_join_clause_check;
extern bool g_trace_pushed_down_clauses_collation;
extern bool g_trace_transform_vars;
extern char *g_rewrite_enabled_for_tables;
extern double g_rewrite_minimum_cost;

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
extern set_join_pathlist_hook_type next_set_join_pathlist_hook;

#endif /* exension_h */
