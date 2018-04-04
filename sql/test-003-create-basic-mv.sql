create materialized view test_mv1 as select key, count(value) FROM test GROUP BY key;

refresh materialized view test_mv1;

insert into pgx_rewritable_matviews values ('public', 'test_mv1', array['public.test']);
