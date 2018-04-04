-- create FDW server

DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback FOREIGN DATA WRAPPER pg_fdw_mv_rewrite
            OPTIONS (dbname '$$||current_database()||$$',
                port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;

alter server loopback options (add use_remote_estimate 'true');

CREATE USER MAPPING FOR public SERVER loopback;

create table pgx_rewritable_matviews (matviewschemaname name, matviewname name, tables text[]);
