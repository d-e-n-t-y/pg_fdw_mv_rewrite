explain (VERBOSE, COSTS OFF) select key, COUNT (value) * 19 from test_lb group by key;

select key, COUNT (value) * 19 from test_lb group by key;

explain (VERBOSE, COSTS OFF) select key, COUNT (value) - 13 from test_lb group by key;

select key, COUNT (value) - 13 from test_lb group by key;

explain (VERBOSE, COSTS OFF) select key, 'Numbering ' || COUNT (value) || '.' from test_lb group by key;

select key, 'Numbering ' || COUNT (value) || '.' from test_lb group by key;
