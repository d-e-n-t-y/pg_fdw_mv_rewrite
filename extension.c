//
//  exension.c
//  mv_rewrite
//
//  Created by John Dent on 28/04/2018.
//  Copyright © 2018 John Dent. All rights reserved.
//

#include "postgres.h"

#include "extension.h"
#include "postgres_fdw.h"
#include "utils/guc.h"

extern create_upper_paths_hook_type create_upper_paths_hook;

/*
 * Old hook values.
 *
 * We will chain to these (if set) after we've done our work. It will
 * be set to the current value (if any) of the hook during initialization.
 */
create_upper_paths_hook_type next_create_upper_paths_hook = NULL;

/*
 * GUC parameters.
 */
bool g_log_match_progress;
bool g_trace_match_progress;
bool g_trace_parse_select_query;
bool g_trace_group_clause_source_check;
bool g_trace_having_clause_source_check;
bool g_trace_where_clause_source_check;
bool g_trace_select_clause_source_check;
bool g_trace_join_clause_check;
bool g_debug_join_clause_check;


void
_PG_init (void)
{
	next_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = mv_rewrite_create_upper_paths_hook;
	
	DefineCustomBoolVariable("mv_rewrite.log_match_progress",
							 gettext_noop("Log progress through matching a candidate materialized view against the query being executed."),
							 NULL,
							 &g_log_match_progress,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("mv_rewrite.trace_match_progress",
							 gettext_noop("Trace progress through matching a candidate materialized view against the query being executed."),
							 NULL,
							 &g_trace_match_progress,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("mv_rewrite.trace_parse_select_query",
							 gettext_noop("Trace the parsing of the subquery that scans the materialized view."),
							 NULL,
							 &g_trace_parse_select_query,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("mv_rewrite.trace_group_clause_source_check",
							 gettext_noop("Trace the matching of GROUP BY clauses."),
							 NULL,
							 &g_trace_group_clause_source_check,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("mv_rewrite.trace_having_clause_source_check",
							 gettext_noop("Trace the matching of HAVING clauses."),
							 NULL,
							 &g_trace_having_clause_source_check,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("mv_rewrite.trace_where_clause_source_check",
							 gettext_noop("Trace the matching of WHERE clauses."),
							 NULL,
							 &g_trace_where_clause_source_check,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("mv_rewrite.trace_select_clause_source_check",
							 gettext_noop("Trace the matching of SELECT clauses."),
							 NULL,
							 &g_trace_select_clause_source_check,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("mv_rewrite.trace_join_clause_check",
							 gettext_noop("Trace the matching of JOING clauses."),
							 NULL,
							 &g_trace_join_clause_check,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("mv_rewrite.debug_join_clause_check",
							 gettext_noop("Debug the matching of the GROUP BY clauses."),
							 NULL,
							 &g_debug_join_clause_check,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);
}

void
_PG_fini (void)
{
	create_upper_paths_hook = next_create_upper_paths_hook;
	next_create_upper_paths_hook = NULL;
}
