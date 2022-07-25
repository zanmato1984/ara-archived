#include "ara/expression/unary_op.h"

namespace ara::expression {

std::string UnaryOp::unaryOperatorToString(UnaryOperator op) {
#define UNARY_OP_CASE(OP, PRETTY)                                              \
  case UnaryOperator::OP:                                                      \
    return ARA_STRINGIFY(OP);

  switch (op) {
    APPLY_FOR_UNARY_OPERATORS(UNARY_OP_CASE);
  default:
    ARA_FAIL("Unknown unary op " + std::to_string(static_cast<int32_t>(op)));
  }

#undef UNARY_OP_CASE
}

std::string UnaryOp::unaryOperatorPretty(UnaryOperator op) {
#define UNARY_OP_CASE(OP, PRETTY)                                              \
  case UnaryOperator::OP:                                                      \
    return ARA_STRINGIFY(PRETTY);

  switch (op) {
    APPLY_FOR_UNARY_OPERATORS(UNARY_OP_CASE);
  default:
    ARA_FAIL("Unknown unary op " + std::to_string(static_cast<int32_t>(op)));
  }

#undef UNARY_OP_CASE
}

UnaryOp::UnaryOperator UnaryOp::unaryOperatorFromString(const std::string &s) {
#define UNARY_OP_CASE(OP, PRETTY)                                              \
  if (s == ARA_STRINGIFY(OP)) {                                                \
    return UnaryOperator::OP;                                                  \
  }

  APPLY_FOR_UNARY_OPERATORS(UNARY_OP_CASE)

  ARA_FAIL("Invalid unary operator: " + s);

#undef UNARY_OP_CASE
}

std::shared_ptr<const Column>
UnaryOp::evaluate(const Context &ctx, ThreadId thread_id,
                  const Fragment &fragment) const {
  // TODO: Implement.
  ARA_FAIL("Not implemented");
}

} // namespace ara::expression