create materialized view test_name_value_mv2 as
select n.name, n.class, v.additional
FROM test_name n, test_value v
WHERE n.id1 = v.id1 and n.id2 = v.id2;

refresh materialized view test_name_value_mv2;

select mv_rewrite.enable_rewrite ('test_name_value_mv2');

create materialized view test_name_value_mv3 as
select n.name, n.class, v.additional
FROM test_name n LEFT OUTER JOIN test_value v ON n.id1 = v.id1 and n.id2 = v.id2;

refresh materialized view test_name_value_mv3;

select mv_rewrite.enable_rewrite ('test_name_value_mv3');

create materialized view test_name_value_mv4 as
select n.name, n.class, v.additional
FROM test_name n RIGHT OUTER JOIN test_value v ON n.id1 = v.id1 and n.id2 = v.id2;

refresh materialized view test_name_value_mv4;

select mv_rewrite.enable_rewrite ('test_name_value_mv4');
