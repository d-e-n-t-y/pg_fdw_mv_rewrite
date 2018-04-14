#!/bin/sh

set -e
set -x

export PGPORT=58329

PROGRAM='set -x; set -e;\
$PG_HOME/bin/pg_ctl -m immediate --pgdata=/tmp/$PGPORT stop || true;\
test -e /tmp/$PGPORT && rm -r /tmp/$PGPORT;\
$PG_HOME/bin/initdb --pgdata=/tmp/$PGPORT;\
mv /tmp/$PGPORT/postgresql.conf /tmp/$PGPORT/postgresql.conf.orig;\
sed -e "s/#port = [0-9]*/port = $PGPORT/" < /tmp/$PGPORT/postgresql.conf.orig > /tmp/$PGPORT/postgresql.conf;\
$PG_HOME/bin/pg_ctl --pgdata=/tmp/$PGPORT start;\
make clean && make && make install && make installcheck;\
$PG_HOME/bin/pg_ctl --pgdata=/tmp/$PGPORT stop;\
rm -r /tmp/$PGPORT'

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