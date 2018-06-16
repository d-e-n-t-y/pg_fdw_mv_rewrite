# pg_fdw_mv_rewrite: a short introduction

This is a query rewriting extension for PostgreSQL.

## DISCLAIMER

*NOTE: this is not "production ready" code — if it works for you, then great, but use it after thorough testing, 
and with appropriate caution.*

## Setting up

To get started, load the `mv_rewrite` extension and create a mapping table to allow the `mv_rewrite` extension to know 
what TABLEs can be rewritten, and what MATERIALIZED VIEWs are available to rewrite against.

First load the EXTENSION:

```SQL
postgres=# create extension mv_rewrite;
CREATE EXTENSION
```

Then create the mapping table:

```SQL
postgres=# CREATE TABLE pgx_rewritable_matviews (matviewschemaname name, matviewname name, tables text[]);
CREATE TABLE
```

Note: at present, it is necessary to create the mapping table even if there are no MATERIALIZED VIEWs to rewrite against. 
(This is a limitation that should be removed at some point.)

## Enabling basic diagnostics

Query rewrite can happen transparently. However, to understand what is going on, it can be helpful to enable some basic
feedback.

Enable progress logging during the matching process:

```SQL
postgres=# set mv_rewrite.log_match_progress = 'true';
SET
```

## Basic use case

Given a simple TALBE and data, for example:

```SQL
postgres=# create table test (key text, hidden text, value text);
CREATE TABLE
postgres=# insert into test (select 'key1', 'hidden1', generate_series (1, 10));
INSERT 0 10
postgres=# insert into test (select 'key2', 'hidden2', generate_series (1, 1e6));
INSERT 0 1000000
```

And a supporting MATERIALIZED VIEW, for example:

```SQL
postgres=# create materialized view test_mv1 as select key, count(value) FROM test GROUP BY key;
SELECT 2
```

A simple GROUP BY aggregate query against `test` results in a plan of some 10k, for example:

```SQL
postgres=# explain select key, COUNT (value) from test group by key;
                                          QUERY PLAN                                           
-----------------------------------------------------------------------------------------------
 Finalize GroupAggregate  (cost=13620.28..13620.31 rows=1 width=13)
   Group Key: key
...
```

If `test` and its MATERIALIZED VIEW (`test_mv1`) are added for rewrite as follows:

```SQL
postgres=# insert into pgx_rewritable_matviews values ('public', 'test_mv1', array['public.test']);
INSERT 0 1
```

Now aggregate queries targetting `test` will consider if they can be satisfied instead by `test_mv1`, 
as demonstrated in the following query plan:

```SQL
postgres=# explain (verbose) select key, COUNT (value) from test group by key;
                                                                  QUERY PLAN                                                                   
-----------------------------------------------------------------------------------------------------------------------------------------------
 Custom Scan (MVRewriteScan)  (cost=0.00..22.00 rows=1200 width=13)
   Output: key, (count(value))
   Rewritten: scan of public.test_mv1
   Original costs: cost=13620.28..13620.31 rows=1 width=13; cost=21370.15..21370.16 rows=1 width=13; cost=133119.51..140619.60 rows=1 width=13
```

## Limitations

Please heed the warning about this not being production ready!

Query rewrite is only considered for queries involving simple GROUP BY aggregates. (Although a similar approach might speed
complex joins and sort operations too, they are not yet considered for rewrite.)

## Configurable parameters

### Minimum cost for rewrite

Rewriting cam be a costly planning operation, and it may be undesirable to apply the query rewriting search for plans 
that are already relatively cheap. A threshold can be set beneath which rewrite will not be attempted.

For example:

```SQL
postgres=# set mv_rewrite.rewrite_minimum_cost = '1000000.0';
SET
```

A minimum cost of 1,000,000 means that only relatively costly queries will considered for rewriting.

The default (`-1`) causes rewrite to be considered for all supported queries.

If you have `mv_rewrite.log_match_progress` enabled, then the fact of rewriting not being considered for reason of 
already having a plan of acceptable cost in hand is announced as follows:

```SQL
INFO:  mv_rewrite_create_upper_paths_hook: already have path with acceptable cost.
```

### Confining rewrite to certain tables only

It may be desirable that rewrite is applied only to a certain set of large tables, or to tables that are typically
subjected to complex queries. This limitation can be configured by by setting `mv_rewrite.rewrite_enabled_for_tables`.

The SETting `mv_rewrite.rewrite_enabled_for_tables` to `DEFAULT` results in rewrite being considered for all 
tables, whereas an empty string means it is effectively disabled.

```SQL
set mv_rewrite.rewrite_enabled_for_tables = 'public.table1,public.table2';
```

If you have `mv_rewrite.log_match_progress` enabled, then the fact of rewriting not being considered for reason of 
the query targetting tables not enabled for rewrite is announced as follows:

```SQL
INFO:  mv_rewrite_add_rewritten_mv_paths: MV rewrite not enabled for one or more table in the query.
```

## History

This extension was originally written as an extension to the Foreign Data Wrapper for accessing remote Postgres 
databases, but has since morphed into a regular Postgres EXTENSION, allowing any queries targetting any regular 
TABLE to be rewritten to instead read from a MATERIALIZED VIEW. At some point, I'll probably rename it.
