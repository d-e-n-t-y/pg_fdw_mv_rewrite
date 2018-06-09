-- Anticipate message "query requires unsupported features"
set mv_rewrite.log_match_progress = 'true';

set mv_rewrite.log_match_progress = 'true';
explain (VERBOSE, COSTS OFF)
select distinct key, hidden, count (value)
  from test
  group by key, hidden;

select distinct key, hidden, count (value)
  from test
  group by key, hidden;
