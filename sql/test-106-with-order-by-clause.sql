-- FIXME: wrt ORDER BY: hmmm... this seems to be fine... but not really sure why it is working.
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test_lb group by key order by key;

select key, COUNT (value) from test_lb group by key order by key;

explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test_lb group by key order by key desc;

select key, COUNT (value) from test_lb group by key order by key desc;
