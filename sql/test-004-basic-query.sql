explain (VERBOSE, COSTS OFF) select * from (select key, COUNT (value) from test group by key) x;
