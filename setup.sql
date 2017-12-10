create extension postgres_fdw;

create server local_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1', port '5433', dbname 'denty');

CREATE USER MAPPING FOR denty server local_server OPTIONS (user 'denty', password 'password');

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

CREATE FOREIGN TABLE events_all_remote (timestamp timestamp with time zone, hc1 text, hc2 text, addr smallint, ctrl smallint, category text, subcategory text, advices jsonb, warnings text, detail float) server local_server OPTIONS (schema_name 'public', table_name 'events_all');

explain verbose select ts_to_bucket (timestamp, 'minute', 15), avg (detail) 
from events_all_remote
where
    category = 'BATTERY_LEVEL' and subcategory = 'N/A'
    and ts_to_bucket (timestamp, 'minute', 15) >= '2017-11-01 00:00:00+00' 
    and ts_to_bucket (timestamp, 'minute', 15) < '2017-11-25 00:00:00+00'
    and (hc1, hc2) = (select hc1, hc2 from room where name = 'Cold room')
group by hc1, hc2, ts_to_bucket (timestamp, 'minute', 15)
order by ts_to_bucket (timestamp, 'minute', 15);

alter server local_server options (add extensions 'ts');

alter server local_server options (add use_remote_estimate 'true');

CREATE FOREIGN TABLE room_remote (name character varying(64), hc1 text, hc2 text) server local_server OPTIONS (schema_name 'public', table_name 'room');

alter table public.pgx_rewritable_matviews add column tables text[];

update public.pgx_rewritable_matviews t set tables = array[t.tableschemaname || '.' || t.tablename];

alter table public.pgx_rewritable_matviews drop column tableschemaname;

alter table public.pgx_rewritable_matviews drop column tablename;

insert into pgx_rewritable_matviews values ('public', 'events_all_room_rollup_15m', array['public.events_all', 'public.room']);

CREATE FOREIGN TABLE event_remote (timestamp timestamp with time zone, hc1 text, hc2 text, addr smallint, ctrl smallint, category text, subcategory text, advices jsonb, warnings text, detail float) server local_server OPTIONS (schema_name 'public', table_name 'event');

alter foreign table event_remote add column id integer;

insert into pgx_rewritable_matviews values ('public', 'event_sdr_device', array['public.event']);
