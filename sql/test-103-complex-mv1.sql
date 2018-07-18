-- Anticipate no rewrite
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test where hidden = 'hidden2' group by key;

select key, COUNT (value) from test where hidden = 'hidden2' group by key;

-- Anticipate rewrite
explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test where hidden = 'hidden2' group by key, hidden;

select key, COUNT (value) from test where hidden = 'hidden2' group by key, hidden;
