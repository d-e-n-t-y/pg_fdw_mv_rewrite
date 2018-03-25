#!/bin/sh

set -e

export PG_HOME=/opt/local/lib/postgresql10

PORT=58329

test -e /tmp/$PORT && rm -r /tmp/$PORT

$PG_HOME/bin/initdb --pgdata=/tmp/$PORT

mv /tmp/$PORT/postgresql.conf /tmp/$PORT/postgresql.conf.orig

sed -e "s/#port = [0-9]*/port = $PORT/" < /tmp/$PORT/postgresql.conf.orig > /tmp/$PORT/postgresql.conf

$PG_HOME/bin/pg_ctl --pgdata=/tmp/$PORT start

cp setup.sql /tmp/$PORT/setup.sql.orig

sed -e "s/port '[0-9]*'/port '$PORT'/" -e "s/dbname '[0-9A-Za-z]*'/dbname 'postgres'/" < /tmp/$PORT/setup.sql.orig > /tmp/$PORT/setup.sql

$PG_HOME/bin/psql --port=$PORT postgres --file=/tmp/$PORT/setup.sql

$PG_HOME/bin/pg_ctl --pgdata=/tmp/$PORT stop

rm -r /tmp/$PORT
