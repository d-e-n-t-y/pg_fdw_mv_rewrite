create materialized view test_mv5 as select key, count(value) FROM test GROUP BY key order by key;

refresh materialized view test_mv5;

insert into pgx_rewritable_matviews values ('public', 'test_mv5', array['public.test']);
