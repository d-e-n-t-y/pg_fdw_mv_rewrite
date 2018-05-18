explain (VERBOSE, COSTS OFF) select key, 'key=' || key, COUNT (value) from test group by key;

select key, 'key=' || key, COUNT (value) from test group by key;
