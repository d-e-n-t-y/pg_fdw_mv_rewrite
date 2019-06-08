create materialized view test_mv2 as
select key, hidden, count(value)
FROM test
where hidden = 'hidden2'
GROUP BY key, hidden;

refresh materialized view test_mv2;

select mv_rewrite.enable_rewrite ('test_mv2');
