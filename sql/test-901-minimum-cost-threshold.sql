-- Anticipate rewrite
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate rewrite
set mv_rewrite.rewrite_minimum_cost = '1.0';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate no rewrite
set mv_rewrite.rewrite_minimum_cost = '1000000000';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate message "already have path with acceptable cost"
set mv_rewrite.log_match_progress = 'true';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;
