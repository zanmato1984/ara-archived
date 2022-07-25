#pragma once

#include "ara/expression/expressions.h"

namespace ara::expression {

#define APPLY_FOR_BINARY_OPERATORS(ACTION)                                     \
  ACTION(ADD, +, add)                                                          \
  ACTION(SUB, -, substract)                                                    \
  ACTION(MUL, *, multiply)                                                     \
  ACTION(DIV, /, divide)                                                       \
  ACTION(MOD, %, mod)                                                          \
  ACTION(POW, ^, power)                                                        \
  ACTION(EQUAL, =, equal)                                                      \
  ACTION(NOT_EQUAL, !=, not_equal)                                             \
  ACTION(LESS, <, less)                                                        \
  ACTION(GREATER, >, greater)                                                  \
  ACTION(LESS_EQUAL, <=, less_equal)                                           \
  ACTION(GREATER_EQUAL, >=, greater_equal)                                     \
  ACTION(BITWISE_AND, &, bit_wise_and)                                         \
  ACTION(BITWISE_OR, |, bit_wise_or)                                           \
  ACTION(BITWISE_XOR, ^, bit_wise_xor)                                         \
  ACTION(LOGICAL_AND, &&, and_kleene)                                          \
  ACTION(LOGICAL_OR, ||, or_kleene)                                            \
  ACTION(COALESCE, coalesce, coalesce)                                         \
  ACTION(SHIFT_LEFT, <<, shift_left)                                           \
  ACTION(SHIFT_RIGHT, >>, shift_right)                                         \
  ACTION(LOG_BASE, log, logb)                                                  \
  ACTION(ATAN2, atan2, atan2)

#define DEF_BINARY_OPERATOR_ENUM(OP, PRETTY, ARROW) OP,

enum class BinaryOperator : int32_t {
  APPLY_FOR_BINARY_OPERATORS(DEF_BINARY_OPERATOR_ENUM)
};

#undef DEF_BINARY_OPERATOR_ENUM

inline std::string binaryOperatorToString(BinaryOperator op) {
#define BINARY_OP_CASE(OP, PRETTY, ARROW)                                      \
  case BinaryOperator::OP:                                                     \
    return ARA_STRINGIFY(OP);

  switch (op) {
    APPLY_FOR_BINARY_OPERATORS(BINARY_OP_CASE);
  default:
    ARA_FAIL("Unknown binary op " + std::to_string(static_cast<int32_t>(op)));
  }

#undef BINARY_OP_CASE
}

inline std::string binaryOperatorPretty(BinaryOperator op) {
#define BINARY_OP_CASE(OP, PRETTY, ARROW)                                      \
  case BinaryOperator::OP:                                                     \
    return ARA_STRINGIFY(PRETTY);

  switch (op) {
    APPLY_FOR_BINARY_OPERATORS(BINARY_OP_CASE);
  default:
    ARA_FAIL("Unknown binary op " + std::to_string(static_cast<int32_t>(op)));
  }

#undef BINARY_OP_CASE
}

inline BinaryOperator binaryOperatorFromString(const std::string &s) {
#define BINARY_OP_CASE(OP, PRETTY, ARROW)                                      \
  if (s == ARA_STRINGIFY(OP)) {                                                \
    return BinaryOperator::OP;                                                 \
  }

  APPLY_FOR_BINARY_OPERATORS(BINARY_OP_CASE)

  ARA_FAIL("Invalid binary operator: " + s);

#undef BINARY_OP_CASE
}

struct BinaryOp : public Op {
  BinaryOp(BinaryOperator binary_operator_,
           std::shared_ptr<const Expression> left,
           std::shared_ptr<const Expression> right, DataType data_type)
      : Op({left, right}, data_type), binary_operator(binary_operator_) {
    ARA_ASSERT(left, "Invalid left operand for BinaryOp");
    ARA_ASSERT(right, "Invalid right operand for BinaryOp");
  }

  const DataType &dataType() const override { return data_type; }

  std::shared_ptr<const Column>
  evaluate(const Context &ctx, ThreadId thread_id,
           const Fragment &fragment) const override;

  std::string name() const override { return "BinaryOp"; }

  std::string toString() const override {
    auto pretty = binaryOperatorPretty(binary_operator);
    if (pretty.empty()) {
      return binaryOperatorToString(binary_operator) + "(" +
             operands_[0]->toString() + ", " + operands_[1]->toString() + ")";
    }
    return operands_[0]->toString() + " " + pretty + " " +
           operands_[1]->toString();
  }

  BinaryOperator binaryOperator() const { return binary_operator; }

  std::shared_ptr<const Expression> left() const { return operands_[0]; }

  std::shared_ptr<const Expression> right() const { return operands_[1]; }

private:
  BinaryOperator binary_operator;
};

} // namespace ara::expression
