# contrib/pg_fdw_mv_rewrite/Makefile

MODULE_big = pg_fdw_mv_rewrite
OBJS = postgres_fdw.o option.o deparse.o connection.o shippable.o equalwalker.o $(WIN32RES)
PGFILEDESC = "pg_fdw_mv_rewrite - MV-rewriting foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = pg_fdw_mv_rewrite
DATA = pg_fdw_mv_rewrite--0.1.sql

REGRESS = pg_fdw_mv_rewrite

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/pg_fdw_mv_rewrite
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
