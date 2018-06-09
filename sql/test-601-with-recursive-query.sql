-- Anticipate message "query requires unsupported features"
set mv_rewrite.log_match_progress = 'true';

explain (VERBOSE, COSTS OFF)
with recursive data as (
    select 1 n, key, value
      from test
      where key = 'key1'
      and value = 1::text
  union all
    select curr.n + 1, next.key, next.value
      from data curr, test next
      where
        curr.key = next.key
        and next.value = (curr.value::numeric + 1)::text
)
select key, count (value)
  from data
  group by key;

with recursive data as (
    select 1 n, key, value
      from test
      where key = 'key1'
      and value = 1::text
  union all
    select curr.n + 1, next.key, next.value
      from data curr, test next
      where
        curr.key = next.key
        and next.value = (curr.value::numeric + 1)::text
)
select key, count (value)
  from data
  group by key;
