# mv_rewrite

MODULE_big = mv_rewrite.0.6.1
OBJS = mv_rewrite.o equalwalker.o extension.o join_is_legal.o build_joinrel_restrictlist.o release_rewrite_locks.o $(WIN32RES)
PGFILEDESC = "mv_rewrite - MV rewrite extension for PostgreSQL"
TESTS = $(wildcard sql/*.sql)

equalwalker.c: generate_equalswalker.sh
	./generate_equalswalker.sh > equalwalker.c

clean: local-clean

local-clean:
	rm -f equalwalker.c

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = mv_rewrite
DATA = mv_rewrite--0.6.sql \
    mv_rewrite--0.6--0.6.1.sql mv_rewrite--0.6.1.sql

REGRESS = $(patsubst sql/%.sql,%,$(TESTS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/mv_rewrite
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
