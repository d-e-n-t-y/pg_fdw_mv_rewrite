explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test_lb where key = 'key2' group by key;

select key, COUNT (value) from test_lb where key = 'key2' group by key;
