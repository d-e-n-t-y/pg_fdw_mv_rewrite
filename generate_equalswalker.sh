#!/bin/sh

#  generate_equalswalker.sh
#  PostgreSQL
#
#  Created by John Dent on 01/07/2018.
#  Copyright © 2018 John Dent. All rights reserved.

cat ${PG_HOME}/src/backend/nodes/equalfuncs.c | \
	sed -e '/^_equal.*(.*,$/ { N; s/\n/\\n/; }' | \
	sed -e 's/^equal(\(.*\))$/equal_tree_walker(\1, bool (*walker) (), void *context)/' \
		-e 's/^\(_equal.*(.*\))$/\1, bool (*walker) (), void *context)/' \
		-e 's/\(retval = _equal.*(.*\));$/\1, walker, context);/' \
		-e 's/\(if (!_equal.*(.*\)))/\1, walker, context))/' \
		-e 's/if (!equal(\(.*\)))/if (!walker(\1, context))/' \
		-e 's/^\(#include "postgres.h"\)$/\1\\n#include "equalwalker.h"/' | \
	sed -e 's/\\n/\
/g'
