-- Anticipate rewrite
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate rewrite
set mv_rewrite.rewrite_enabled_for_tables = 'public.test';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate no rewrite
set mv_rewrite.rewrite_enabled_for_tables = '';
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN
	explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

-- Anticipate rewrite
set mv_rewrite.rewrite_enabled_for_tables = ' public.test, public.test   ,     public.test     ';
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

-- Anticipate no rewrite
set mv_rewrite.rewrite_enabled_for_tables = 'public.not_test';
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN
	explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

-- Anticipate no rewrite
set mv_rewrite.rewrite_enabled_for_tables = 'other_schema.test';
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN
	explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

-- Anticipate message "MV rewrite not enabled for one or more table in the query"
set mv_rewrite.log_match_progress = 'true';
-- We don't actually care about the plan itself, only the log output.
DO LANGUAGE plpgsql $$ BEGIN EXECUTE 'explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;'; END; $$;

-- Anticipate rewrite
set mv_rewrite.log_match_progress = 'false';
set mv_rewrite.rewrite_enabled_for_tables to default;
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;
