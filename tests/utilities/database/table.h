#pragma once

#include "ara/data/fragment.h"

namespace ara::test::database {

using ara::data::Fragment;

using TableId = int64_t;

struct Table {
  TableId id;
  std::vector<std::shared_ptr<const Fragment>> fragments;
};

} // namespace ara::test::database