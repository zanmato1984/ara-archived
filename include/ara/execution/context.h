#pragma once

#include "ara/common/types.h"
#include "ara/execution/memory_resource.h"

namespace ara::driver {
struct Option;
} // namespace ara::driver

namespace ara::execution {

using ara::driver::Option;

struct Context {
  Context(const Option &option_);

  const Option &option;
  std::unique_ptr<MemoryResource> memory_resource;
};

} // namespace ara::execution
