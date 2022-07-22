#pragma once

#include "ara/expression/expressions.h"
#include "ara/kernel/kernel.h"

namespace ara::kernel {

using ara::type::Schema;

using ara::expression::Expression;

struct Project : NonSourceStreamKernel {
  Project(KernelId id,
          std::vector<std::shared_ptr<const Expression>> expressions_)
      : NonSourceStreamKernel(id), expressions(std::move(expressions_)) {}

  std::string name() const override { return "Project"; }

  std::string toString() const override {
    return Kernel::toString() + "(" +
           std::accumulate(expressions.begin() + 1, expressions.end(),
                           expressions[0]->toString(),
                           [](const auto &all, const auto &e) {
                             return all + ", " + e->toString();
                           }) +
           ")";
  }

protected:
  std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment) const override;

private:
  std::vector<std::shared_ptr<const Expression>> expressions;
};

} // namespace ara::kernel