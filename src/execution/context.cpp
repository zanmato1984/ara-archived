#include "ara/execution/context.h"

namespace ara::execution {

Context::Context(const Option &option_)
    : option(option_), memory_resource(createMemoryResource(option)) {}

} // namespace ara::execution