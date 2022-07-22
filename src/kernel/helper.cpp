#include "helper.h"
#include "ara/data/column_scalar.h"
#include "ara/data/column_vector.h"

namespace cura::kernel::detail {

using cura::data::Column;
using cura::data::ColumnScalar;
using cura::data::ColumnVector;
using cura::data::createArrowColumnScalar;
using cura::data::createArrowColumnVector;

std::shared_ptr<Fragment>
concatFragments(MemoryResource::Underlying *underlying, const Schema &schema,
                const std::vector<std::shared_ptr<const Fragment>> &fragments) {
  std::vector<std::shared_ptr<const Column>> columns;
  for (size_t i_col = 0; i_col < fragments.front()->numColumns(); i_col++) {
    const auto &column = fragments.front()->column<Column>(i_col);
    if (auto cv = std::dynamic_pointer_cast<const ColumnVector>(column); cv) {
      std::vector<std::shared_ptr<arrow::Array>> arrays;
      const auto &data_type = fragments.front()->column(i_col)->dataType();
      for (const auto &fragment : fragments) {
        const auto &col = fragment->column(i_col);
        arrays.emplace_back(col->arrow());
      }
      const auto &concat =
          CURA_GET_ARROW_RESULT(arrow::Concatenate(arrays, underlying));
      auto concated = createArrowColumnVector(data_type, concat);
      columns.emplace_back(std::move(concated));
    } else if (auto cs = std::dynamic_pointer_cast<const ColumnScalar>(column);
               cs) {
      size_t size = 0;
      const auto &data_type =
          fragments.front()->column<ColumnScalar>(i_col)->dataType();
      for (const auto &fragment : fragments) {
        auto next_cs = fragment->column<ColumnScalar>(i_col);
        CURA_ASSERT(next_cs && next_cs->arrow()->Equals(cs->arrow()),
                    "Mismatched column scalar between fragments");
        size += next_cs->size();
      }
      auto concated = createArrowColumnScalar(data_type, size, cs->arrow());
      columns.emplace_back(std::move(concated));
    } else {
      CURA_FAIL("Neither column vector nor column scalar");
    }
  }
  return std::make_shared<Fragment>(std::move(columns));
}

} // namespace cura::kernel::detail
