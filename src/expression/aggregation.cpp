#include "ara/expression/aggregation.h"

namespace ara::expression {

std::shared_ptr<const Column>
Aggregation::evaluate(const Context &ctx, ThreadId thread_id,
                      const Fragment &fragment) const {
  ARA_FAIL("Shouldn't call Aggregation evaluate directly");
}

} // namespace ara::expression
