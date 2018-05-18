-- FIXME: re-grouping based on a strict superset of MV group by terms is not currently supported

explain (VERBOSE, COSTS OFF) select key, hidden, COUNT (value) from test group by key, hidden order by key;

select key, hidden, COUNT (value) from test group by key, hidden order by key;
