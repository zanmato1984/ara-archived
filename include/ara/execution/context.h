#pragma once

#include "ara/common/types.h"
#include "ara/execution/memory_resource.h"

namespace cura::driver {
struct Option;
} // namespace cura::driver

namespace cura::execution {

using cura::driver::Option;

struct Context {
  Context(const Option &option_);

  const Option &option;
  std::unique_ptr<MemoryResource> memory_resource;
};

} // namespace cura::execution
