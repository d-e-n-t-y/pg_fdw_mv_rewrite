-- Anticipate rewrite
explain (verbose, costs off) select key, COUNT (value) from test group by key order by key;

-- Anticipate rewrite
explain (verbose, costs off) select key, COUNT (value) from test group by key order by 1;

-- Anticipate rewrite
explain (verbose, costs off) select key, COUNT (value) from test group by key order by key asc;

-- Anticipate rewrite, but not using _mv5
explain (verbose, costs off) select key, COUNT (value) from test group by key order by key desc;

-- Anticipate rewrite
set mv_rewrite.log_match_progress = true;
explain (verbose, costs off) select key, COUNT (value) from test group by key order by key asc nulls last;

-- Anticipate rewrite
explain (verbose, costs off) select key, COUNT (value) from test group by key order by key asc nulls last;

-- Anticipate rewrite, but not using _mv5
explain (verbose, costs off) select key, COUNT (value) from test group by key order by key, count (value);

-- Anticipate rewrite, but not using _mv5
explain (verbose, costs off) select key, COUNT (value) from test group by key order by count (value), key;
