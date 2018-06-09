-- Anticipate message "query requires unsupported features"
set mv_rewrite.log_match_progress = 'true';

explain (VERBOSE, COSTS OFF)
select t.key, COUNT (lt.value)
  from
    test t,
    lateral (
      select key, value
        from test tt
        where t.key = tt.key
          and tt.value = (t.value::numeric + 1)::text
	) lt
  where t.value::numeric < 50
  group by t.key;

select t.key, COUNT (lt.value)
  from
    test t,
    lateral (
      select key, value
        from test tt
        where t.key = tt.key
          and tt.value = (t.value::numeric + 1)::text
	) lt
  where t.value::numeric < 50
  group by t.key;
