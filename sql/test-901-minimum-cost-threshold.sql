-- Anticipate rewrite
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate rewrite
set mv_rewrite.rewrite_minimum_cost = '1.0';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate no rewrite
set mv_rewrite.rewrite_minimum_cost = '1000000000';
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN explain (VERBOSE, COSTS OFF)
	select key, COUNT (value) from test group by key order by key
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

-- Anticipate message "already have path with acceptable cost"
set mv_rewrite.log_match_progress = 'true';
-- We don't actually care about the plan itself, only the log output.
DO LANGUAGE plpgsql $$ BEGIN EXECUTE 'explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;'; END; $$;
