/* contrib/pg_fdw_mv_rewrite/pg_fdw_mv_rewrite--0.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fdw_mv_rewrite" to load this file. \quit

CREATE or replace FUNCTION pg_fdw_mv_rewrite_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME.0.3'
LANGUAGE C STRICT;

CREATE or replace FUNCTION pg_fdw_mv_rewrite_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME.0.3'
LANGUAGE C STRICT;

-- No upgrade actions necessary.
