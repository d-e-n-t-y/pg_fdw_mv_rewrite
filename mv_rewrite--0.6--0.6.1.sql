-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mv_rewrite" to load this file. \quit

// FIXME: this upgrade script fails to work; there is no equivalent of the LOAD to unload the prior
// module version. Therefore, when the LOAD executes, it errors due to the GUC parameters being
// redefined.

DO LANGUAGE plpgsql $$BEGIN
EXECUTE 'alter database ' || current_database() || ' set session_preload_libraries = ''MODULE_PATHNAME.0.6.1''';
END;
$$;

LOAD 'MODULE_PATHNAME.0.6.1';

-- No upgrade actions necessary.
