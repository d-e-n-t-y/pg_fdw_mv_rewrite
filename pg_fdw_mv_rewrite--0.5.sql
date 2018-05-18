/* contrib/pg_fdw_mv_rewrite/pg_fdw_mv_rewrite--0.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fdw_mv_rewrite" to load this file. \quit

DO LANGUAGE plpgsql $$BEGIN
EXECUTE 'alter database ' || current_database() || ' set session_preload_libraries = ''MODULE_PATHNAME.0.5''';
END;
$$;

LOAD 'MODULE_PATHNAME.0.5';
