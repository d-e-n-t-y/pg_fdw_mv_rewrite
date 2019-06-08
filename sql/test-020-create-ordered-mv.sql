create materialized view test_mv5 as select key, count(value) FROM test GROUP BY key order by key;

refresh materialized view test_mv5;

select mv_rewrite.enable_rewrite ('test_mv5');
