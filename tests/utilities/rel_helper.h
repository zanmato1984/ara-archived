#pragma once

#include "ara/relational/rels.h"

namespace ara::test::relational {

using ara::relational::Rel;

template <typename RelType, typename... Args>
inline std::shared_ptr<const Rel> makeRel(Args &&...args) {
  return std::make_shared<RelType>(std::forward<Args>(args)...);
}

std::string toJson(std::shared_ptr<const Rel> rel);

} // namespace ara::test::relational
