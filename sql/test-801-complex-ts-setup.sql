create table room (
       name varchar(64),
       hc1 text,
       hc2 text
);

alter table room add constraint room_pk primary key (hc1, hc2);

create table event (
 timestamp timestamp with time zone,
 timestr character varying(24),
 addr smallint,
 ctrl smallint,
 category text,
 subcategory text,
 advices jsonb,
 warnings text,
 detail double precision,
 intermediate_decoding character varying(256),
 decode_summary character varying(256),
 hc1 text,
 hc2 text,
 id integer
);

create index event_cat_sub_time on event (category, subcategory, timestamp);
create index event_idx_id on event(id);
create index event_session_id_sdr_device
       on event ((advices is not null),
       	  	 (advices ? 'session_id'),
		 (advices ? 'sdr_device'),
		 ((advices ->> 'session_id')::text::integer),
		 ((advices ->> 'sdr_device')::text));
create index event_time on event (timestamp);
alter table event add constraint event_room_fk foreign key (hc1, hc2)
      references room (hc1, hc2);

create or replace view event_sdr_device as
 SELECT esd_e."timestamp",
    esd_e.timestr,
    esd_e.addr,
    esd_e.ctrl,
    esd_e.category,
    esd_e.subcategory,
    esd_e.advices,
    esd_e.warnings,
    esd_e.detail,
    esd_e.intermediate_decoding,
    esd_e.decode_summary,
    esd_e.hc1,
    esd_e.hc2,
    esd_e.id,
    esd_e.session_id,
    esd_s.sdr_device
   FROM ( SELECT esd_ei."timestamp",
            esd_ei.timestr,
            esd_ei.addr,
            esd_ei.ctrl,
            esd_ei.category,
            esd_ei.subcategory,
            esd_ei.advices,
            esd_ei.warnings,
            esd_ei.detail,
            esd_ei.intermediate_decoding,
            esd_ei.decode_summary,
            esd_ei.hc1,
            esd_ei.hc2,
            esd_ei.id,
            (esd_ei.advices ->> 'session_id'::text)::integer AS session_id
           FROM event esd_ei
          WHERE esd_ei.advices IS NOT NULL) esd_e,
    ( SELECT (esd_si.advices ->> 'session_id'::text)::integer AS session_id,
            esd_si.advices ->> 'sdr_device'::text AS sdr_device
           FROM event esd_si
          WHERE esd_si.advices IS NOT NULL AND esd_si.advices ? 'session_id'::text AND esd_si.advices ? 'sdr_device'::text) esd_s
  WHERE esd_e.session_id = esd_s.session_id AND esd_e.id >= esd_s.session_id AND esd_e.id < (esd_s.session_id + 10);

create or replace view event_sdr_device_signal as
 SELECT event_sdr_device.hc1,
    event_sdr_device.hc2,
    event_sdr_device."timestamp",
    'SIGNAL'::text AS category,
    event_sdr_device.sdr_device AS subcategory,
    1.0 AS detail
   FROM event_sdr_device;

create or replace view events_all as
SELECT event."timestamp",
    timestamp::text as timestr,
    event.hc1,
    event.hc2,
    event.addr,
    event.ctrl,
    event.category,
    event.subcategory,
    event.advices,
    event.warnings,
    event.detail,
    null::text as intermediate_decoding,
    null::text as decode_summary
   FROM event
UNION ALL
 SELECT event_sdr_device_signal."timestamp",
    event_sdr_device_signal."timestamp"::text AS timestr,
    event_sdr_device_signal.hc1,
    event_sdr_device_signal.hc2,
    NULL::smallint AS addr,
    NULL::smallint AS ctrl,
    'SIGNAL',
    event_sdr_device_signal.subcategory,
    NULL::jsonb AS advices,
    NULL::text AS warnings,
    event_sdr_device_signal.detail,
    NULL::character varying AS intermediate_decoding,
    NULL::character varying AS decode_summary
   FROM event_sdr_device_signal;

create or replace view events_all_room as
  select e.timestamp, e.hc1, e.hc2, e.category, e.subcategory, e.advices, e.warnings, e.detail, r.name room_name
    from events_all e, room r
    where e.hc1 = r.hc1 and e.hc2 = r.hc2;

create or replace function ts_to_bucket (ts timestamp with time zone,
                                         unit text, bucket_size int)
    returns timestamp with time zone
language plpgsql immutable leakproof
as $$
begin
   return date_trunc (unit, ts) -
                mod (date_part (unit, date_trunc (unit, ts))::int, bucket_size) *
                ('1 ' || unit)::interval;
end;
$$;

create or replace function ts_series_bucket (start_ts timestamp with time zone,
                                             end_ts timestamp with time zone,
                                             unit text, bucket_size int)
                    returns table (ts_bucket timestamp with time zone,
                                   ts_bucket_next timestamp with time zone)
                    language sql stable
as $$
 select ts_to_bucket (s, unit, bucket_size) ts_bucket,
     ts_to_bucket (s, unit, bucket_size) + (bucket_size || ' '|| unit)::interval ts_bucket_next
   from generate_series (ts_to_bucket (start_ts, unit, bucket_size),
                         ts_to_bucket (end_ts, unit, bucket_size),
                         (bucket_size || ' '|| unit)::interval) s
   where s < end_ts
$$;

create or replace function ordered_ts (start_ts timestamp with time zone,
                                       end_ts timestamp with time zone,
                                       my_room_name text,
                                       my_category text, my_subcategory text,
                                       unit text, bucket_size int)
                    returns table ("timestamp" timestamp with time zone,
                                   value float,
                                   stddev_detail float, min_detail float, max_detail float,
                                   count_measurements bigint,
                                   max_low_battery float)
                    language sql stable
as $$
     select
        ts.ts_bucket as timestamp,
        sum (avg_detail*count_measurements)/sum (count_measurements) avg_detail,
        avg (stddev_detail) stddev_detail,
        min (min_detail) min_detail, max (max_detail) max_detail,
        sum (count_measurements)::bigint count_measurements,
        0::float max_low_battery
     from
       ts_series_bucket (start_ts, end_ts, unit, bucket_size) ts
     left outer join
       (select
            ts_to_bucket (d.timestamp, unit, bucket_size) as ts_bucket,
            avg (d.detail::float) avg_detail, stddev_pop (d.detail::float) stddev_detail,
            min (d.detail::float) min_detail, max (d.detail::float) max_detail,
            count (d.detail) count_measurements
         from
           events_all_room d
         where
           ts_to_bucket (d.timestamp, unit, bucket_size) >= start_ts
           and ts_to_bucket (d.timestamp, unit, bucket_size) < end_ts
         group by
           ts_to_bucket (d.timestamp, unit, bucket_size), d.category, d.subcategory, d.room_name
		 having
		   d.room_name = my_room_name
           and d.category = my_category and d.subcategory = my_subcategory
       ) g
       on ts.ts_bucket = g.ts_bucket
     group by ts.ts_bucket
     order by ts.ts_bucket asc
$$;

insert into room values ('Room 1', '5', '7');

insert into event (timestamp, hc1, hc2, category, subcategory, detail)
select t.t, '5', '7', 'TEMPERATURE', 'N/A', row_number() over (order by t.t)/200
	from generate_series ('2018-01-01 00:00:00+00'::timestamp with time zone, '2018-12-25 00:00:00+00'::timestamp with time zone, '1 minute'::interval) t;
insert into event (timestamp, hc1, hc2, category, subcategory, detail)
select t.t, '5', '7', 'HUMIDITY', 'N/A', row_number() over (order by t.t)::float/count(1) over (rows between unbounded preceding and unbounded following)
	from generate_series ('2018-01-01 00:00:00+00'::timestamp with time zone, '2018-12-25 00:00:00+00'::timestamp with time zone, '1 minute'::interval) t;
insert into event (timestamp, hc1, hc2, category, subcategory, detail)
select t.t, '5', '7', 'LUMINOSITY', 'N/A', row_number() over (order by t.t)::float/count(1) over (rows between unbounded preceding and unbounded following)
	from generate_series ('2018-01-01 00:00:00+00'::timestamp with time zone, '2018-12-25 00:00:00+00'::timestamp with time zone, '1 minute'::interval) t;

create materialized view test99 as
 SELECT ts_to_bucket(events_all_room."timestamp", 'minute'::text, 15) AS ts_bucket,
    avg(events_all_room.detail) AS avg_detail,
    stddev_pop(events_all_room.detail) AS stddev_detail,
    min(events_all_room.detail) AS min_detail,
    max(events_all_room.detail) AS max_detail,
    count(events_all_room.detail) AS count_measurements,
    events_all_room.category,
    events_all_room.subcategory,
    r.name AS room_name
   FROM events_all events_all_room,
    room r
  WHERE events_all_room.hc1 = r.hc1 AND events_all_room.hc2 = r.hc2 AND events_all_room.category = 'TEMPERATURE'::text
  GROUP BY (ts_to_bucket(events_all_room."timestamp", 'minute'::text, 15)), events_all_room.category, events_all_room.subcategory, r.name;

select mv_rewrite.enable_rewrite ('test99');

analyse event;
analyse test99;
