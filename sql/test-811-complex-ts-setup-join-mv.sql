create materialized view events_all_room_mv as select * from events_all_room;

insert into pgx_rewritable_matviews values ('public', 'events_all_room_mv', '{public.event,public.room,public.event_sdr_device,public.event_sdr_device_signal,public.events_all,public.events_all_room}');
