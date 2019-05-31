explain (verbose, costs off)
select timestamp, to_char (value, '999999D9999') as value
	from ordered_ts ('2018-06-09 00:00:00+01', '2018-06-10 00:00:00+01', 'Room 1', 'TEMPERATURE', 'N/A', 'minute', 15);

select timestamp, to_char (value, '999999D9999') as value
from ordered_ts ('2018-06-09 00:00:00+01', '2018-06-10 00:00:00+01', 'Room 1', 'TEMPERATURE', 'N/A', 'minute', 15);
