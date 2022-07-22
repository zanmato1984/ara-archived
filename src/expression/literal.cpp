#include "ara/expression/literal.h"
#include "ara/data/column_factories.h"
#include "ara/data/column_scalar.h"
#include "ara/data/fragment.h"

namespace cura::expression {

using cura::data::createArrowColumnScalar;

std::shared_ptr<const Column>
Literal::evaluate(const Context &ctx, ThreadId thread_id,
                  const Fragment &fragment) const {
  return createArrowColumnScalar(data_type, fragment.size(), scalar);
}

std::string Literal::toString() const {
  return scalar->ToString();
}

} // namespace cura::expression
