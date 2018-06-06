explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key;

select key, COUNT (value) from test group by key order by key;

explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test group by key order by key desc;

select key, COUNT (value) from test group by key order by key desc;
