-- Anticipate message "expr (...) not found in MV tlist"
set mv_rewrite.log_match_progress = 'true';

explain (VERBOSE, COSTS OFF) select key, COUNT (key) from test group by key;

select key, COUNT (key) from test group by key;
