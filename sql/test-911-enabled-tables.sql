-- Anticipate rewrite
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate rewrite
set mv_rewrite.rewrite_enabled_for_tables = 'public.test';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate no rewrite
set mv_rewrite.rewrite_enabled_for_tables = '';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate rewrite
set mv_rewrite.rewrite_enabled_for_tables = ' public.test, public.test   ,     public.test     ';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate no rewrite
set mv_rewrite.rewrite_enabled_for_tables = 'public.not_test';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate no rewrite
set mv_rewrite.rewrite_enabled_for_tables = 'other_schema.test';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate message "MV rewrite not enabled for one or more table in the query"
set mv_rewrite.log_match_progress = 'true';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate rewrite
set mv_rewrite.log_match_progress = 'false';
set mv_rewrite.rewrite_enabled_for_tables to default;
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;
