# mv_rewrite

MODULE_big = pg_fdw_mv_rewrite.0.5
OBJS = postgres_fdw.o deparse.o shippable.o equalwalker.o extension.o join_is_legal.o $(WIN32RES)
PGFILEDESC = "mv_rewrite - MV rewrite extension for PostgreSQL"
TESTS = $(wildcard sql/*.sql)

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = pg_fdw_mv_rewrite
DATA = pg_fdw_mv_rewrite--0.1.sql \
	pg_fdw_mv_rewrite--0.2.2.sql pg_fdw_mv_rewrite--0.1--0.2.2.sql \
	pg_fdw_mv_rewrite--0.3.sql pg_fdw_mv_rewrite--0.2.2--0.3.sql \
	pg_fdw_mv_rewrite--0.4.sql pg_fdw_mv_rewrite--0.3--0.4.sql \
	pg_fdw_mv_rewrite--0.5.sql pg_fdw_mv_rewrite--0.4--0.5.sql

REGRESS = $(patsubst sql/%.sql,%,$(TESTS))

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
