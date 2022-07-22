#pragma once

#include <list>
#include <memory>
#include <string>
#include <vector>

namespace ara::driver {
struct Option;
} // namespace ara::driver

namespace ara::execution {
struct Pipeline;
} // namespace ara::execution

namespace ara::relational {
struct Rel;
} // namespace ara::relational

namespace ara::planning {

using ara::driver::Option;
using ara::execution::Pipeline;
using ara::relational::Rel;

struct Planner {
  explicit Planner(const Option &option_) : option(option_) {}

  std::list<std::unique_ptr<Pipeline>>
  plan(const std::shared_ptr<const Rel> &rel) const;

  std::vector<std::string> explain(const std::shared_ptr<const Rel> &rel,
                                   bool extended = false) const;

private:
  const Option &option;
};

} // namespace ara::planning