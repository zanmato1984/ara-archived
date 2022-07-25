#include "ara/expression/binary_op.h"
#include "ara/data/column_factories.h"
#include "ara/data/column_scalar.h"
#include "ara/data/column_vector.h"

#include <algorithm>
#include <arrow/compute/api.h>

namespace ara::expression {

using ara::data::ColumnScalar;
using ara::data::ColumnVector;
using ara::data::createArrowColumnVector;
using ara::type::TypeId;

namespace detail {

template <typename LeftColumn, typename RightColumn>
std::shared_ptr<const Column>
dispatchBinaryOperator(const Context &ctx, ThreadId thread_id,
                       LeftColumn &&left, RightColumn &&right,
                       BinaryOperator binary_operator,
                       const DataType &result_type) {
  auto arrow_func = [&]() {
#define BINARY_OP_CASE(OP, PRETTY, ARROW)                                      \
  case BinaryOperator::OP:                                                     \
    return ARA_STRINGIFY(ARROW);

    switch (binary_operator) {
      APPLY_FOR_BINARY_OPERATORS(BINARY_OP_CASE);
    default:
      ARA_FAIL("Unknown binary op " +
               std::to_string(static_cast<int32_t>(binary_operator)));
    }
  }();

  arrow::compute::ExecContext context(
      ctx.memory_resource->preConcatenate(thread_id));
  const auto &res = ARA_GET_ARROW_RESULT(arrow::compute::CallFunction(
      arrow_func, {left->arrow(), right->arrow()}, &context));
  return createArrowColumnVector(result_type, res.make_array());
}

std::shared_ptr<const Column> dispatchBinaryOpColumns(
    const Context &ctx, ThreadId thread_id, std::shared_ptr<const Column> left,
    std::shared_ptr<const Column> right, BinaryOperator binary_operator,
    const DataType &result_type) {
  auto left_cv = std::dynamic_pointer_cast<const ColumnVector>(left);
  auto right_cv = std::dynamic_pointer_cast<const ColumnVector>(right);
  auto left_cs = std::dynamic_pointer_cast<const ColumnScalar>(left);
  auto right_cs = std::dynamic_pointer_cast<const ColumnScalar>(right);

  if (left_cv && right_cv) {
    return dispatchBinaryOperator(ctx, thread_id, left_cv, right_cv,
                                  binary_operator, result_type);
  } else if (left_cv && right_cs) {
    return dispatchBinaryOperator(ctx, thread_id, left_cv, right_cs,
                                  binary_operator, result_type);
  } else if (left_cs && right_cv) {
    return dispatchBinaryOperator(ctx, thread_id, left_cs, right_cv,
                                  binary_operator, result_type);
  } else if (left_cs && right_cs) {
    ARA_FAIL("Unimplemented");
  }

  ARA_FAIL("Shouldn't reach here");
}

std::shared_ptr<const Column>
evaluateBinaryOp(const Context &ctx, ThreadId thread_id,
                 std::shared_ptr<const Column> left,
                 std::shared_ptr<const Column> right,
                 BinaryOperator binary_operator, const DataType &result_type) {
  return dispatchBinaryOpColumns(ctx, thread_id, left, right, binary_operator,
                                 result_type);
}

} // namespace detail

std::shared_ptr<const Column>
BinaryOp::evaluate(const Context &ctx, ThreadId thread_id,
                   const Fragment &fragment) const {
  auto left = operands_[0]->evaluate(ctx, thread_id, fragment);
  auto right = operands_[1]->evaluate(ctx, thread_id, fragment);
  ARA_ASSERT(left, "Left column of binary op is null");
  ARA_ASSERT(right, "Right column of binary op is null");

  return detail::evaluateBinaryOp(ctx, thread_id, left, right, binary_operator,
                                  data_type);
}

} // namespace ara::expression
