-- Anticipate no rewrite
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN explain (VERBOSE, COSTS OFF)
	select 'key=' || key, COUNT (value) from test group by 'key=' || key
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

select 'key=' || key, COUNT (value) from test group by 'key=' || key;

-- Anticipate no rewrite
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN explain (VERBOSE, COSTS OFF)
	select 'key=' || substr (key, 1, 3), COUNT (value) from test group by 'key=' || substr (key, 1, 3)
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

select 'key=' || substr (key, 1, 3), COUNT (value) from test group by 'key=' || substr (key, 1, 3);
