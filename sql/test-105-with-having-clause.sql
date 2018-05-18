explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key having count (value) > 100;

select key, COUNT (value) from test group by key having count (value) > 100;
