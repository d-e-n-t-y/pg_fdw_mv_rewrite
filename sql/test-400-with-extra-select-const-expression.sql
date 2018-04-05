explain (VERBOSE, COSTS OFF) select key, 'key=' || key, COUNT (value) from test_lb group by key;

select key, 'key=' || key, COUNT (value) from test_lb group by key;
