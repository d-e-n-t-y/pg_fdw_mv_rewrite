/* contrib/pg_fdw_mv_rewrite/pg_fdw_mv_rewrite--0.4.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fdw_mv_rewrite" to load this file. \quit

DROP FUNCTION pg_fdw_mv_rewrite_handler() cascade;

DROP FUNCTION pg_fdw_mv_rewrite_validator(text[], oid) cascade;

DROP FOREIGN DATA WRAPPER pg_fdw_mv_rewrite cascade;

DO LANGUAGE plpgsql $$BEGIN
EXECUTE 'alter database ' || current_database() || ' set session_preload_libraries = ''MODULE_PATHNAME.0.5''';
END;
$$;

LOAD 'MODULE_PATHNAME.0.5';

-- No upgrade actions necessary.
