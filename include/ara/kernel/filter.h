#pragma once

#include "ara/expression/expressions.h"
#include "ara/kernel/kernel.h"

namespace ara::kernel {

using ara::type::Schema;

using ara::expression::Expression;

struct Filter : NonSourceStreamKernel {
  Filter(KernelId id, Schema schema_,
         std::shared_ptr<const Expression> condition_)
      : NonSourceStreamKernel(id), schema(std::move(schema_)),
        condition(condition_) {}

  std::string name() const override { return "Filter"; }

  std::string toString() const override {
    return Kernel::toString() + "(" + condition->toString() + ")";
  }

protected:
  std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment) const override;

private:
  Schema schema;
  std::shared_ptr<const Expression> condition;
};

} // namespace ara::kernel
