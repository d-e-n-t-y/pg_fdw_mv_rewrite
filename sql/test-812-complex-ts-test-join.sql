set mv_rewrite.log_match_progress = 'true';
explain (verbose, costs off)
select * from events_all_room;
