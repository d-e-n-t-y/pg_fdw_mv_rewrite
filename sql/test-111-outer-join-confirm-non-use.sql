explain (VERBOSE, COSTS OFF)
select n.name, n.class, v.additional, count(value) as count
FROM test_name n LEFT OUTER JOIN test_value v ON n.id1 = v.id1 and n.id2 = v.id2
GROUP BY n.name, n.class, v.additional;

--select n.name, n.class, v.additional, count(value) as count
--FROM test_name n, test_value v
--WHERE n.id1 = v.id1 and n.id2 = v.id2
--GROUP BY n.name, n.class, v.additional;
