#pragma once

#include "ara/relational/rels.h"
#include "utilities/database/database.h"

#include <iostream>
#include <list>

namespace ara::test::execution {

using ara::relational::Rel;
using ara::test::database::Database;

template <typename DbExecutor>
void testExecute(const Database<DbExecutor> &db, const std::string &json) {
  db.explain(json, true);
  db.execute(json);
}

template <typename DbExecutor>
void testExecute(const Database<DbExecutor> &db,
                 std::shared_ptr<const Rel> rel) {
  db.explain(rel, true);
  db.execute(rel);
}

} // namespace ara::test::execution
