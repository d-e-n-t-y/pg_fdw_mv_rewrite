create materialized view test_union_mv1 as
select key, count::text from test_mv1
  union all
  select key, quantity from test_expr_mv1;

refresh materialized view test_union_mv1;

create materialized view test_union_mv2 as
select key,
case when count(value) < 10 then 'several'
when count(value) < 100 then 'tens'
when count(value) < 1000 then 'hundreds'
when count(value) < 10000 then 'thousands'
when count(value) < 100000 then 'tens of thousands'
when count(value) < 1000000 then 'hundreds of thousands'
when count(value) < 10000000 then 'millions'
else 'many'
end as quantity
FROM test GROUP BY key
union all
select key, count(value)::text FROM test GROUP BY key;

refresh materialized view test_union_mv2;

select mv_rewrite.enable_rewrite ('test_union_mv1');
select mv_rewrite.enable_rewrite ('test_union_mv2');
