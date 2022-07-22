#pragma once

#include "ara/common/errors.h"
#include "ara/data/column.h"

#include <optional>

namespace ara::data {

struct ColumnScalar : public Column {
  explicit ColumnScalar(DataType data_type, size_t size)
      : Column(std::move(data_type)), size_(size) {}

  size_t size() const override { return size_; }

  virtual std::shared_ptr<arrow::Scalar> arrow() const = 0;

protected:
  size_t size_;
};

struct ColumnScalarArrow : public ColumnScalar {
  ColumnScalarArrow(DataType data_type, size_t size,
                    std::shared_ptr<arrow::Scalar> scalar_)
      : ColumnScalar(std::move(data_type), size), scalar(scalar_) {}

  std::shared_ptr<arrow::Scalar> arrow() const override { return scalar; }

private:
  std::shared_ptr<arrow::Scalar> scalar;
};

} // namespace ara::data
