-- Anticipate no rewrite (because the WHERE clause does not match the MV)
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN explain (VERBOSE, COSTS OFF)
	select key, hidden, COUNT (value) from test group by key, hidden order by key
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

select key, hidden, COUNT (value) from test group by key, hidden order by key;
