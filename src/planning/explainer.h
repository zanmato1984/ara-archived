#pragma once

#include "ara/relational/rel_visitor.h"

namespace ara::planning {

using ara::relational::Rel;
using ara::relational::RelVisitor;

struct Explainer : public RelVisitor<Explainer, std::vector<std::string>> {
  std::vector<std::string>
  defaultVisit(const std::shared_ptr<const Rel> &rel,
               const std::vector<std::vector<std::string>> &children);
};

} // namespace ara::planning
