create materialized view test_name_value_mv1 as
select n.name, n.class, v.additional, count(value) as count
FROM test_name n, test_value v
WHERE n.id1 = v.id1 and n.id2 = v.id2
GROUP BY n.name, n.class, v.additional;

refresh materialized view test_name_value_mv1;

select mv_rewrite.enable_rewrite ('test_name_value_mv1');
