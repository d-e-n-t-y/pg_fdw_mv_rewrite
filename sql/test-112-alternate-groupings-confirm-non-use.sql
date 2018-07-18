-- Anticipate no rewrite because expression is not in any MV's GROUP BY list
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN explain (VERBOSE, COSTS OFF)
	select 'key=' || key, COUNT (value) from test group by 'key=' || key
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

select 'key=' || key, COUNT (value) from test group by 'key=' || key;

-- Anticipate no rewrite because expression is not in any MV's GROUP BY list
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN explain (VERBOSE, COSTS OFF)
	select 'key=' || substr (key, 1, 3), COUNT (value) from test group by 'key=' || substr (key, 1, 3)
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

select 'key=' || substr (key, 1, 3), COUNT (value) from test group by 'key=' || substr (key, 1, 3);

-- Anticipate no rewrite because GROUP BY clauses are ordered wrong for test_mv2
DO LANGUAGE plpgsql $$ DECLARE t text; BEGIN FOR t IN explain (VERBOSE, COSTS OFF)
	select key, hidden from test where hidden = 'hidden2' group by hidden, key
LOOP IF t LIKE '%Rewritten%' THEN RAISE 'Rewritten'; END IF; END LOOP; END; $$;

select key, hidden from test where hidden = 'hidden2' group by hidden, key;
