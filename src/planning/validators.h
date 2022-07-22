#pragma once

#include "ara/relational/rel_visitor.h"

namespace ara::planning {

using ara::relational::RelAggregate;
using ara::relational::RelFilter;
using ara::relational::RelHashJoin;
using ara::relational::RelProject;
using ara::relational::RelSort;
using ara::relational::RelVisitor;

/// Validate that ColumnRef is within the bound of the children's output and of
/// the same data type as the referred column.
struct ColumnRefValidator : public RelVisitor<ColumnRefValidator, void> {
  void visitFilter(const std::shared_ptr<const RelFilter> &filter);

  void visitHashJoin(const std::shared_ptr<const RelHashJoin> &hash_join);

  void visitProject(const std::shared_ptr<const RelProject> &project);

  void visitAggregate(const std::shared_ptr<const RelAggregate> &aggregate);

  void visitSort(const std::shared_ptr<const RelSort> &sort);
};

// TODO: Type check and inference.

} // namespace ara::planning