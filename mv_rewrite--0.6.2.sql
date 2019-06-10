-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mv_rewrite" to load this file. \quit

DO LANGUAGE plpgsql $$BEGIN
EXECUTE 'alter database ' || current_database() || ' set session_preload_libraries = ''MODULE_PATHNAME.0.6.2''';
END;
$$;

LOAD 'MODULE_PATHNAME.0.6.2';

CREATE TABLE pgx_rewritable_matviews (
	matviewschemaname name, -- schema containing MV
	matviewname name, -- name of MV
	tables text[] -- tables involved in MV's query (used to aid matching during the rewrite process)
);


-- Create a function that always returns the first non-NULL item
CREATE OR REPLACE FUNCTION first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $1;
$$;

-- And then wrap an aggregate around it
CREATE AGGREGATE FIRST (
        sfunc    = mv_rewrite.first_agg,
        basetype = anyelement,
        stype    = anyelement
);

CREATE FUNCTION enable_rewrite (in relation name) RETURNS void
AS $$
	with recursive mv_depend as (
	select m.oid mv_oid,
	       rw.oid rewrite_oid,
	       mv_rewrite.first (dc.oid) ref_obj_oid
  	from
	    (select oid, * from pg_catalog.pg_class where relkind in ('m'::"char", 'v'::"char")) m
	    join (select oid, * from pg_catalog.pg_rewrite where ev_type = '1'::char) rw on (m.oid = rw.ev_class)
	    join pg_catalog.pg_depend rd on (rw.oid = rd.objid)
	    join pg_catalog.pg_class dc on (rd.refobjid = dc.oid)
  	where
	    rd.refobjid <> m.oid
	  group by
	    m.oid, m.relname, m.relnamespace, rw.oid, dc.oid
	  order by
	    m.oid, m.relname, m.relnamespace, rw.oid, dc.oid
	),
	mv_trans_depend as (
	  select mv_oid, ref_obj_oid from mv_depend
	  union all
	  select p.mv_oid, c.ref_obj_oid from mv_trans_depend p join mv_depend c on (p.ref_obj_oid = c.mv_oid)
	),
	rewrite_record as (
	  select
		mvn.nspname relnamespace,
		mvc.relname,
		array_agg (distinct rn.nspname || '.' || mvr.relname) ref_relnames
	  from
		pg_catalog.pg_class mvc
		join pg_namespace mvn on (mvc.relnamespace = mvn.oid)
		join mv_trans_depend mvd on (mvc.oid = mvd.mv_oid)
		join pg_class mvr on (mvd.ref_obj_oid = mvr.oid)
		join pg_namespace rn on (mvr.relnamespace = rn.oid)
	  where
	    mvc.oid = (select objid from pg_catalog.pg_get_object_address ('materialized view', parse_ident($1), '{}'))
		and mvr.relkind in ('r'::"char", 'v'::"char")
	  group by
	      mvn.nspname, mvc.relname
	)
	insert into mv_rewrite.pgx_rewritable_matviews (matviewschemaname, matviewname, tables)
	       select relnamespace, relname, ref_relnames from rewrite_record;
$$
LANGUAGE SQL;


-- No upgrade actions necessary.
