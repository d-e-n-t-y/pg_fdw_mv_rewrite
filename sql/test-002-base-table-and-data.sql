create table test (key text, hidden text, value text);

insert into test (select 'key1', 'hidden1', generate_series (1, 10));

insert into test (select 'key2', 'hidden2', generate_series (1, 1e6));
