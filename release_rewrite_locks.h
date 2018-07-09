//
//  release_rewrite_locks.h
//  mv_rewrite
//
//  Created by John Dent on 09/07/2018.
//  Copyright © 2018 John Dent. All rights reserved.
//

#ifndef release_rewrite_locks_h
#define release_rewrite_locks_h

#include <stdio.h>

#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rowsecurity.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

extern void
ReleaseRewriteLocks(Query *parsetree,
					bool forExecute,
					bool forUpdatePushedDown);

#endif /* release_rewrite_locks_h */
