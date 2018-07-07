create materialized view events_all_room_mv as
  SELECT e."timestamp",
	e.hc1, e.hc2,
	e.category, e.subcategory,
	e.advices, e.warnings,
	e.detail,
	r.name AS room_name
  FROM events_all e,
	room r
  WHERE
	e.hc1 = r.hc1 AND e.hc2 = r.hc2;

insert into pgx_rewritable_matviews values ('public', 'events_all_room_mv', '{public.event,public.room,public.event_sdr_device,public.event_sdr_device_signal,public.events_all,public.events_all_room}');
