#pragma once

#include "ara/driver/option.h"
#include "ara/execution/context.h"

using ara::driver::Option;
using ara::execution::Context;

inline Context makeTrivialContext(const Option &option) {
  return Context(option);
}
