explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test_lb where hidden = 'hidden2' group by key;

select key, COUNT (value) from test_lb where hidden = 'hidden2' group by key;
