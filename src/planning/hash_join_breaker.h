#pragma once

#include "rel_deep_copy_visitor.h"

namespace ara::planning {

/// Break HashJoin within the given Rel.
struct HashJoinBreaker : public RelDeepCopyVisitor<HashJoinBreaker> {
  using Base = RelDeepCopyVisitor<HashJoinBreaker>;
  std::shared_ptr<const Rel>
  deepCopyHashJoin(const std::shared_ptr<const RelHashJoin> &hash_join,
                   const std::vector<std::shared_ptr<const Rel>> &children);
};

} // namespace ara::planning