#pragma once

#include "ara/type/data_type.h"

#include <arrow/api.h>

namespace ara::data {

using ara::type::DataType;

class ColumnScalar;
class ColumnVector;

std::unique_ptr<ColumnScalar>
createArrowColumnScalar(const DataType &data_type, size_t size,
                        std::shared_ptr<arrow::Scalar> scalar);

std::unique_ptr<ColumnVector>
createArrowColumnVector(const DataType &data_type,
                        std::shared_ptr<arrow::Array> array);

} // namespace ara::data
