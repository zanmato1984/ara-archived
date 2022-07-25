#include "hash_join_breaker.h"
#include "ara/expression/binary_op.h"
#include "ara/expression/expression_visitor.h"

namespace ara::planning {

using ara::expression::BinaryOp;
using ara::expression::BinaryOperator;
using ara::expression::ColumnRef;
using ara::expression::Expression;
using ara::expression::ExpressionVisitor;
using ara::expression::Op;
using ara::relational::BuildSide;
using ara::relational::RelHashJoinBuild;
using ara::relational::RelHashJoinProbe;

namespace detail {

using JoinKeys = std::pair<std::vector<std::shared_ptr<const ColumnRef>>,
                           std::vector<std::shared_ptr<const ColumnRef>>>;

struct JoinConditionParser
    : public ExpressionVisitor<JoinConditionParser, void> {
  JoinConditionParser(const std::shared_ptr<const RelHashJoin> &hash_join_)
      : hash_join(hash_join_) {}

  void visitColumnRef(const std::shared_ptr<const ColumnRef> &column_ref) {
    if (column_ref->columnIdx() < hash_join->left()->output().size()) {
      join_keys.first.emplace_back(column_ref);
    } else {
      join_keys.second.emplace_back(std::make_shared<ColumnRef>(
          column_ref->columnIdx() - hash_join->left()->output().size(),
          column_ref->dataType()));
    }
  }

  void visitOp(const std::shared_ptr<const Op> &op) {
    auto binary_op = std::dynamic_pointer_cast<const BinaryOp>(op);
    if (!binary_op) {
      ARA_FAIL("Invalid op in join condition: " + op->toString());
    }
    switch (binary_op->binaryOperator()) {
    case BinaryOperator::EQUAL:
      ARA_ASSERT(
          std::dynamic_pointer_cast<const ColumnRef>(binary_op->left()) &&
              std::dynamic_pointer_cast<const ColumnRef>(binary_op->right()),
          "Join condition must be column = column: " + binary_op->toString());
      break;
    case BinaryOperator::LOGICAL_AND: {
      auto left = std::dynamic_pointer_cast<const BinaryOp>(binary_op->left());
      auto right =
          std::dynamic_pointer_cast<const BinaryOp>(binary_op->right());
      ARA_ASSERT(left &&
                     (left->binaryOperator() == BinaryOperator::EQUAL ||
                      left->binaryOperator() == BinaryOperator::LOGICAL_AND) &&
                     right &&
                     (right->binaryOperator() == BinaryOperator::EQUAL ||
                      right->binaryOperator() == BinaryOperator::LOGICAL_AND),
                 "Join condition must be all conjunctions: " +
                     binary_op->toString());
    } break;
    default:
      ARA_FAIL("Invalid binary operator in join condition: " +
               binary_op->toString());
    }
  }

  void defaultVisit(const std::shared_ptr<const Expression> &e) {
    ARA_FAIL("Invalid join condition: " + e->toString());
  }

  std::shared_ptr<const RelHashJoin> hash_join;
  JoinKeys join_keys;
};

JoinKeys
parseJoinCondition(const std::shared_ptr<const RelHashJoin> &hash_join) {
  JoinConditionParser parser(hash_join);
  parser.visit(hash_join->condition());
  auto join_keys = std::move(parser.join_keys);
  ARA_ASSERT(join_keys.first.size() == join_keys.second.size(),
             "Mismatched left and right keys " +
                 std::to_string(join_keys.first.size()) + ":" +
                 std::to_string(join_keys.second.size()));
  for (size_t i = 0; i < join_keys.first.size(); i++) {
    ARA_ASSERT(join_keys.first[i]->dataType() ==
                   join_keys.second[i]->dataType(),
               "Mismatched type between left and right key " +
                   join_keys.first[i]->dataType().toString() + ":" +
                   join_keys.second[i]->dataType().toString());
  }
  return join_keys;
}

} // namespace detail

std::shared_ptr<const Rel> HashJoinBreaker::deepCopyHashJoin(
    const std::shared_ptr<const RelHashJoin> &hash_join,
    const std::vector<std::shared_ptr<const Rel>> &children) {
  ARA_ASSERT(hash_join->inputs.size() == 2, "Invalid RelHashJoin node");
  auto keys = detail::parseJoinCondition(hash_join);
  auto build_side = hash_join->buildSide();
  auto build = std::make_shared<RelHashJoinBuild>(
      build_side == BuildSide::LEFT ? children[0] : children[1],
      build_side == BuildSide::LEFT ? std::move(keys.first)
                                    : std::move(keys.second));
  return std::make_shared<RelHashJoinProbe>(
      hash_join->joinType(),
      build_side == BuildSide::LEFT ? children[1] : children[0], build,
      build_side == BuildSide::LEFT ? std::move(keys.second)
                                    : std::move(keys.first),
      hash_join->output(), build_side);
}

} // namespace ara::planning
