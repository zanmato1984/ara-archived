#include "ara/expression/ti_unary_op.h"
#include "ara/data/column_vector.h"
#include "extract_year.h"

namespace ara::expression {

using ara::data::ColumnVector;

std::string TiUnaryOp::unaryOperatorToString(UnaryOperator op) {
#define UNARY_OP_CASE(OP, PRETTY)                                              \
  case UnaryOperator::OP:                                                      \
    return ARA_STRINGIFY(OP);

  switch (op) {
    APPLY_FOR_TI_UNARY_OPERATORS(UNARY_OP_CASE);
  default:
    ARA_FAIL("Unknown ti unary op " +
              std::to_string(static_cast<int32_t>(op)));
  }

#undef UNARY_OP_CASE
}

std::string TiUnaryOp::unaryOperatorPretty(UnaryOperator op) {
#define UNARY_OP_CASE(OP, PRETTY)                                              \
  case UnaryOperator::OP:                                                      \
    return ARA_STRINGIFY(PRETTY);

  switch (op) {
    APPLY_FOR_TI_UNARY_OPERATORS(UNARY_OP_CASE);
  default:
    ARA_FAIL("Unknown ti unary op " +
              std::to_string(static_cast<int32_t>(op)));
  }

#undef UNARY_OP_CASE
}

TiUnaryOp::UnaryOperator
TiUnaryOp::unaryOperatorFromString(const std::string &s) {
#define UNARY_OP_CASE(OP, PRETTY)                                              \
  if (s == ARA_STRINGIFY(OP)) {                                               \
    return UnaryOperator::OP;                                                  \
  }

  APPLY_FOR_TI_UNARY_OPERATORS(UNARY_OP_CASE)

  ARA_FAIL("Invalid ti unary operator: " + s);

#undef UNARY_OP_CASE
}

namespace detail {

template <typename OperandColumn>
std::shared_ptr<const Column> dispatchTiUnaryOperator(
    const Context &ctx, ThreadId thread_id, OperandColumn &&operand,
    TiUnaryOp::UnaryOperator unary_operator, const DataType &result_type) {
  switch (unary_operator) {
  case TiUnaryOp::UnaryOperator::EXTRACT_YEAR:
    return extractYear(ctx, thread_id, std::forward<OperandColumn>(operand),
                       result_type);
  default:
    ARA_FAIL("Unimplemented");
  }
}

std::shared_ptr<const Column>
dispatchTiUnaryOpColumn(const Context &ctx, ThreadId thread_id,
                        std::shared_ptr<const Column> operand,
                        TiUnaryOp::UnaryOperator unary_operator,
                        const DataType &result_type) {
  if (auto operand_cv = std::dynamic_pointer_cast<const ColumnVector>(operand);
      operand_cv) {
    return dispatchTiUnaryOperator(ctx, thread_id, operand_cv, unary_operator,
                                   result_type);
  } else {
    ARA_FAIL("Unimplemented");
  }

  ARA_FAIL("Shouldn't reach here");
}

std::shared_ptr<const Column>
evaluateTiUnaryOp(const Context &ctx, ThreadId thread_id,
                  std::shared_ptr<const Column> operand,
                  TiUnaryOp::UnaryOperator unary_operator,
                  const DataType &result_type) {
  return dispatchTiUnaryOpColumn(ctx, thread_id, operand, unary_operator,
                                 result_type);
}

} // namespace detail

std::shared_ptr<const Column>
TiUnaryOp::evaluate(const Context &ctx, ThreadId thread_id,
                    const Fragment &fragment) const {
  auto operand = operands_[0]->evaluate(ctx, thread_id, fragment);
  ARA_ASSERT(operand, "Operand column of unary op is null");

  return detail::evaluateTiUnaryOp(ctx, thread_id, operand, unary_operator,
                                   data_type);
}

} // namespace ara::expression