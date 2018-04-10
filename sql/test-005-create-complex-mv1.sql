create materialized view test_mv2 as
select key, hidden, count(value)
FROM test
where hidden = 'hidden2'
GROUP BY key, hidden;

refresh materialized view test_mv2;

insert into pgx_rewritable_matviews values ('public', 'test_mv2', array['public.test']);