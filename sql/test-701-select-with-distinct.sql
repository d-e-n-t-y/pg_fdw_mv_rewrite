-- Anticipate message "query requires unsupported features"
-- We don't actually care about the plan itself, only the log output.
set mv_rewrite.log_match_progress = 'true';
set mv_rewrite.rewrite_enabled_for_tables = '';
-- Anticipate no rewrite
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN
	explain (VERBOSE, COSTS OFF)
		select distinct key, hidden, count (value)
  		from test
  		group by key, hidden
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

select distinct key, hidden, count (value)
  from test
  group by key, hidden;
