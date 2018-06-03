/*-------------------------------------------------------------------------
 *
 * postgres_fdw.h
 *		  Foreign-data wrapper for remote PostgreSQL servers
 *
 * Portions Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/postgres_fdw/postgres_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRES_FDW_H
#define POSTGRES_FDW_H

#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/relcache.h"

#include "libpq-fe.h"

/* in postgres_fdw.c */
extern void
pg_mv_rewrite_get_upper_paths(PlannerInfo *root,
			      UpperRelationKind stage,
			      RelOptInfo *input_rel,
			      RelOptInfo *output_rel);

#endif							/* POSTGRES_FDW_H */
