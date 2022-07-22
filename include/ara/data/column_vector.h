#pragma once

#include "ara/common/errors.h"
#include "ara/data/column.h"
#include "ara/data/column_factories.h"

#include <arrow/api.h>

namespace ara::data {

struct ColumnVector : public Column {
  using Column::Column;

  virtual std::unique_ptr<const ColumnVector> view(size_t offset,
                                                   size_t size) const = 0;

  virtual std::shared_ptr<arrow::Array> arrow() const = 0;
};

struct ColumnVectorArrow : public ColumnVector {
  using ColumnVector::ColumnVector;

  ColumnVectorArrow(DataType data_type_,
                    const std::shared_ptr<arrow::Array> array_)
      : ColumnVector(std::move(data_type_)), array(array_) {
    ARA_ASSERT(array, "Underlying column couldn't be null");
    ARA_ASSERT(
        array->type()->Equals(data_type.arrow()),
        "Mismatched data type between column vector and underlying column");
  }

  size_t size() const override { return array->length(); }

  std::unique_ptr<const ColumnVector> view(size_t offset,
                                           size_t size) const override {
    return std::make_unique<ColumnVectorArrow>(data_type,
                                               array->Slice(offset, size));
  }

  std::shared_ptr<arrow::Array> arrow() const override { return array; }

private:
  const std::shared_ptr<arrow::Array> array;
};

} // namespace ara::data
