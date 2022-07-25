#include "ara/expression/expressions.h"
#include "ara/data/column_vector.h"
#include "ara/data/fragment.h"

namespace ara::expression {

std::shared_ptr<const Column>
ColumnRef::evaluate(const Context &ctx, ThreadId thread_id,
                    const Fragment &fragment) const {
  ARA_ASSERT(idx < fragment.numColumns(), "Column idx overflow");
  ARA_ASSERT(fragment.column(idx)->dataType() == data_type,
             "Mismatched data type " +
                 fragment.column(idx)->dataType().toString() + " vs. " +
                 data_type.toString());
  return fragment.column(idx);
}

} // namespace ara::expression