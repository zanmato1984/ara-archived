#pragma once

#include "ara/data/fragment.h"

namespace cura::test::database {

using cura::data::Fragment;

using TableId = int64_t;

struct Table {
  TableId id;
  std::vector<std::shared_ptr<const Fragment>> fragments;
};

} // namespace cura::test::database