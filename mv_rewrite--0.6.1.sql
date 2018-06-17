-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mv_rewrite" to load this file. \quit

DO LANGUAGE plpgsql $$BEGIN
EXECUTE 'alter database ' || current_database() || ' set session_preload_libraries = ''MODULE_PATHNAME.0.6.1''';
END;
$$;

LOAD 'MODULE_PATHNAME.0.6.1';

-- No upgrade actions necessary.
