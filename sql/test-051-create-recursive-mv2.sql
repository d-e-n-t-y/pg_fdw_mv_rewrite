create materialized view test_recursive_mv2 as
  with recursive data as (
    select
        1 n, key, value
      from test
      where
        key = 'key1'
        and value = '1'
    union all
    select
        curr.n + 1, next.key, next.value
      from data curr, test next
      where
        curr.key = next.key
        and next.value = (curr.value::numeric + 1)::text
  )
  select key, value
    from data;

select mv_rewrite.enable_rewrite ('test_recursive_mv2');
