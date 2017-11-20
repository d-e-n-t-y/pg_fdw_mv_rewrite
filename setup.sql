create extension postgres_fdw;

create server local_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1', port '5433', dbname 'denty');

CREATE USER MAPPING FOR denty server local_server OPTIONS (user 'denty', password 'password');

CREATE FOREIGN TABLE test_remote (key text, value text) server local_server OPTIONS (schema_name 'denty', table_name 'test');

create table test (key text, value text);

CREATE FOREIGN TABLE test_remote (key text, value text) server local_server OPTIONS (schema_name 'public', table_name 'test');

create materialized view test_mv1 as select key, count(value) FROM test GROUP BY key;

insert into test (select 'key1', generate_series (1, 10));

insert into test (select 'key2', generate_series (1, 10e6));

refresh materialized view test_mv1;

select * from (select key, COUNT (value) from test_remote group by key) x;

create table pgx_rewritable_matviews (tableschemaname name, tablename name, matviewschemaname name, matviewname name);

insert into pgx_rewritable_matviews values ('public', 'test', 'public', 'test_mv1');

create table test_detail (key text, detail text);

CREATE FOREIGN TABLE test_detail_remote (key text, detail text) server local_server OPTIONS (schema_name 'public', table_name 'test_detail');

insert into test_detail (select 'key1', 'more information');
insert into test_detail (select 'key2', 'less information');

select * from (select key, COUNT (value) from test_remote group by key) kv, (select key, count (detail) from test_detail_remote group by key) d where kv.key = d.key;
