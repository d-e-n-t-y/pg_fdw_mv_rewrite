-- Anticipate message "expr (...) not found in MV tlist"
set mv_rewrite.log_match_progress = 'true';

-- We don't actually care about the plan itself, only the log output.
DO LANGUAGE plpgsql $$BEGIN EXECUTE 'explain (VERBOSE, COSTS OFF) select key, COUNT (key) from test group by key;'; END; $$;

select key, COUNT (key) from test group by key;
