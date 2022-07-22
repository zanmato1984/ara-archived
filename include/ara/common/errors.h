#pragma once

#include <stdexcept>

namespace ara {

struct LogicalError : public std::logic_error {
  explicit LogicalError(const char *message) : std::logic_error(message) {}

  explicit LogicalError(const std::string &message)
      : std::logic_error(message) {}

  // TODO Add an error code member? This would be useful for translating an
  // exception to an error code in a pure-C API
};

} // namespace ara

#define STRINGIFY_DETAIL(x) #x
#define ARA_STRINGIFY(x) STRINGIFY_DETAIL(x)

#define ARA_FAIL(reason)                                                      \
  throw ara::LogicalError("ARA failure at: " __FILE__                        \
                           ":" ARA_STRINGIFY(__LINE__) ": " reason)

#define ARA_ASSERT(cond, reason)                                              \
  (!!(cond)) ? static_cast<void>(0) : ARA_FAIL(reason)

#define ARA_ASSERT_JSON(json)                                                 \
  ARA_ASSERT(json, ARA_STRINGIFY(json) " is false");

#define ARA_ASSERT_ARROW_OK(arrow, reason)                                    \
  {                                                                            \
    const auto &status = (arrow);                                              \
    ARA_ASSERT(status.ok(), reason ": " + status.ToString());                 \
  }

#define ARA_GET_ARROW_RESULT(arrow)                                           \
  [&]() {                                                                      \
    auto &&res = (arrow);                                                      \
    ARA_ASSERT_ARROW_OK(res.status(), "Arrow get result failed");             \
    return std::move(res.ValueUnsafe());                                       \
  }()
