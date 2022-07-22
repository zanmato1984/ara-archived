#pragma once

#include "ara/type/data_type.h"

namespace ara::data {

using ara::type::DataType;

struct Column {
  Column(DataType data_type_) : data_type(std::move(data_type_)) {}

  virtual ~Column() = default;

  const DataType &dataType() const { return data_type; }

  virtual size_t size() const = 0;

protected:
  DataType data_type;
};

} // namespace ara::data
