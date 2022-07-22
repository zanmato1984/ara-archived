#include "ara/data/column_factories.h"
#include "ara/data/column_scalar.h"
#include "ara/data/column_vector.h"

namespace cura::data {

std::unique_ptr<ColumnScalar>
createArrowColumnScalar(const DataType &data_type, size_t size,
                        std::shared_ptr<arrow::Scalar> scalar) {
  return std::make_unique<ColumnScalarArrow>(data_type, size, scalar);
}

std::unique_ptr<ColumnVector>
createArrowColumnVector(const DataType &data_type,
                        std::shared_ptr<arrow::Array> array) {
  return std::make_unique<ColumnVectorArrow>(data_type, array);
}

} // namespace cura::data
