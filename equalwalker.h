//
//  equalswalker.h
//  PostgreSQL
//
//  Created by John Dent on 07/12/2017.
//  Copyright © 2017 John Dent. All rights reserved.
//

#ifndef equalswalker_h
#define equalswalker_h

extern bool
equal_tree_walker(const void *a, const void *b, bool (*walker) (), void *context);

#endif /* equalswalker_h */
