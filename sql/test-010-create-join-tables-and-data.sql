create table test_name (id1 numeric, id2 numeric, name text, class text);

CREATE FOREIGN TABLE test_name_lb (id1 numeric, id2 numeric, name text, class text) server loopback OPTIONS (schema_name 'public', table_name 'test_name');

insert into test_name (select 1, 10, 'key1', 'class1');

insert into test_name (select 2, 20, 'key2', 'class2');

insert into test_name (select 3, 30, 'key3', 'class2');

create table test_value (id1 numeric, id2 numeric, additional text, value text);

CREATE FOREIGN TABLE test_value_lb (id1 numeric, id2 numeric, additional text, value text) server loopback OPTIONS (schema_name 'public', table_name 'test_value');

insert into test_value (select 1, 10, ceil (random()*10), generate_series (1, 100));

insert into test_value (select 2, 20, 200, generate_series (1, 1000));

insert into test_value (select 3, 30, 300, generate_series (1, 10000));

alter table test_name add constraint test_name_pk primary key (id1, id2);

alter table test_value add constraint test_value_fk foreign key (id1, id2) references test_name (id1, id2);
