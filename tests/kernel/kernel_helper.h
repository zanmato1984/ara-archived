#pragma once

#include "ara/driver/option.h"
#include "ara/execution/context.h"

using cura::driver::Option;
using cura::execution::Context;

inline Context makeTrivialContext(const Option &option) {
  return Context(option);
}
