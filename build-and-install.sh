#!/bin/sh

set -e
set -x

export PGPORT=58329

PROGRAM='set -x; set -e;\
make clean && make CFLAGS='-g' && make install'

env PG_HOME=/Users/denty/junk/postgresql-11.0 \
    PATH=/Users/denty/junk/postgresql-11.0/bin:$PATH \
    USE_PGXS=yes \
    bash -c "$PROGRAM"

env PG_HOME=/Users/denty/junk/postgresql-11beta4 \
    PATH=/Users/denty/junk/postgresql-11beta4/bin:$PATH \
    USE_PGXS=yes \
    bash -c "$PROGRAM"

env PG_HOME=/Users/denty/junk/postgresql-11beta3 \
    PATH=/Users/denty/junk/postgresql-11beta3/bin:$PATH \
    USE_PGXS=yes \
    bash -c "$PROGRAM"

env PG_HOME=/Users/denty/junk/postgresql-11beta2 \
    PATH=/Users/denty/junk/postgresql-11beta2/bin:$PATH \
    USE_PGXS=yes \
    bash -c "$PROGRAM"

env PG_HOME=/Users/denty/junk/postgresql-10.3 \
    PATH=/Users/denty/junk/postgresql-10.3/bin:$PATH \
    USE_PGXS=yes \
    bash -c "$PROGRAM"

env PG_HOME=/Users/denty/junk/postgresql-10.2 \
    PATH=/Users/denty/junk/postgresql-10.2/bin:$PATH \
    USE_PGXS=yes \
    bash -c "$PROGRAM"

env PG_HOME=/Users/denty/junk/postgresql-10.1 \
    PATH=/Users/denty/junk/postgresql-10.1/bin:$PATH \
    USE_PGXS=yes \
    bash -c "$PROGRAM"
