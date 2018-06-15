explain (verbose, costs off)
select timestamp, value
	from ordered_ts ('2018-06-09 00:00:00+01', '2018-06-10 00:00:00+01', 'Room 1', 'TEMPERATURE', 'N/A', 'minute', 15);

select timestamp, value
from ordered_ts ('2018-06-09 00:00:00+01', '2018-06-10 00:00:00+01', 'Room 1', 'TEMPERATURE', 'N/A', 'minute', 15);
