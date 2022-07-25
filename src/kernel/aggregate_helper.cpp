#include "aggregate_helper.h"
#include "ara/data/column_scalar.h"
#include "ara/data/column_vector.h"
#include "ara/data/fragment.h"
#include "ara/expression/literal.h"
#include "helper.h"

#include <arrow/compute/api.h>
#include <arrow/visitor.h>
#include <sstream>

namespace ara::kernel::detail {

using ara::data::Column;
using ara::data::ColumnScalar;
using ara::data::ColumnVector;
using ara::expression::Literal;

// TODO: Rewrite these shit in a mature arrow fashion, i.e., array data visitors
// or so.
using ara::data::createArrowColumnVector;

struct AppendScalarTypeVisitor : public arrow::TypeVisitor {
  explicit AppendScalarTypeVisitor(arrow::ArrayBuilder &builder_)
      : builder(builder_) {}

  template <typename ScalarType>
  struct AppendBuilderTypeVisitor : public arrow::TypeVisitor {
    explicit AppendBuilderTypeVisitor(arrow::ArrayBuilder &builder_,
                                      std::shared_ptr<ScalarType> scalar_)
        : builder(builder_), scalar(scalar_) {}

    template <typename BuilderType,
              typename CorrespondingScalarType = typename arrow::TypeTraits<
                  typename BuilderType::TypeClass>::ScalarType>
    auto Append() {
      auto b = dynamic_cast<BuilderType *>(&builder);
      ARA_ASSERT(b, "Cast to concrete builder failed");
      if (scalar->is_valid) {
        auto casted = ARA_GET_ARROW_RESULT(arrow::compute::Cast(
            std::static_pointer_cast<arrow::Scalar>(scalar), builder.type()));
        return b->Append(
            std::dynamic_pointer_cast<CorrespondingScalarType>(casted.scalar())
                ->value);
      } else {
        return b->AppendNull();
      }
    }

    arrow::Status Visit(const arrow::BooleanType &type) {
      return Append<arrow::BooleanBuilder>();
    }

    arrow::Status Visit(const arrow::Int8Type &type) {
      return Append<arrow::Int8Builder>();
    }

    arrow::Status Visit(const arrow::Int16Type &type) {
      return Append<arrow::Int16Builder>();
    }

    arrow::Status Visit(const arrow::Int32Type &type) {
      return Append<arrow::Int32Builder>();
    }

    arrow::Status Visit(const arrow::Int64Type &type) {
      return Append<arrow::Int64Builder>();
    }

    arrow::Status Visit(const arrow::UInt8Type &type) {
      return Append<arrow::UInt8Builder>();
    }

    arrow::Status Visit(const arrow::UInt16Type &type) {
      return Append<arrow::UInt16Builder>();
    }

    arrow::Status Visit(const arrow::UInt32Type &type) {
      return Append<arrow::UInt32Builder>();
    }

    arrow::Status Visit(const arrow::UInt64Type &type) {
      return Append<arrow::UInt64Builder>();
    }

    arrow::Status Visit(const arrow::FloatType &type) {
      return Append<arrow::FloatBuilder>();
    }

    arrow::Status Visit(const arrow::DoubleType &type) {
      return Append<arrow::DoubleBuilder>();
    }

    arrow::Status Visit(const arrow::Date32Type &type) {
      return Append<arrow::Date32Builder>();
    }

    arrow::Status Visit(const arrow::TimestampType &type) {
      return Append<arrow::TimestampBuilder>();
    }

    arrow::Status Visit(const arrow::DurationType &type) {
      return Append<arrow::DurationBuilder>();
    }

    template <typename T> arrow::Status Visit(const T &type) {
      return arrow::Status::NotImplemented(
          "AppendBuilderTypeVisitor not implemented for " +
          builder.type()->ToString() + ": " + scalar->ToString());
    }

    arrow::ArrayBuilder &builder;
    std::shared_ptr<ScalarType> scalar;
  };

  template <typename ScalarType,
            std::enable_if_t<!std::is_same_v<ScalarType, arrow::StringScalar>>
                * = nullptr>
  auto Append() {
    AppendBuilderTypeVisitor visitor(
        builder, std::dynamic_pointer_cast<ScalarType>(scalar));
    return arrow::VisitTypeInline(*builder.type(), &visitor);
  }

  auto Append(std::shared_ptr<arrow::StringScalar> string_scalar) {
    auto b = dynamic_cast<arrow::StringBuilder *>(&builder);
    ARA_ASSERT(b, "Cast to concrete builder failed");
    if (scalar->is_valid) {
      return b->Append(
          static_cast<arrow::util::string_view>(*string_scalar->value));
    } else {
      return b->AppendNull();
    }
  }

  arrow::Status Visit(const arrow::BooleanType &type) {
    return Append<arrow::BooleanScalar>();
  }

  arrow::Status Visit(const arrow::Int8Type &type) {
    return Append<arrow::Int8Scalar>();
  }

  arrow::Status Visit(const arrow::Int16Type &type) {
    return Append<arrow::Int16Scalar>();
  }

  arrow::Status Visit(const arrow::Int32Type &type) {
    return Append<arrow::Int32Scalar>();
  }

  arrow::Status Visit(const arrow::Int64Type &type) {
    return Append<arrow::Int64Scalar>();
  }

  arrow::Status Visit(const arrow::UInt8Type &type) {
    return Append<arrow::UInt8Scalar>();
  }

  arrow::Status Visit(const arrow::UInt16Type &type) {
    return Append<arrow::UInt16Scalar>();
  }

  arrow::Status Visit(const arrow::UInt32Type &type) {
    return Append<arrow::UInt32Scalar>();
  }

  arrow::Status Visit(const arrow::UInt64Type &type) {
    return Append<arrow::UInt64Scalar>();
  }

  arrow::Status Visit(const arrow::FloatType &type) {
    return Append<arrow::FloatScalar>();
  }

  arrow::Status Visit(const arrow::DoubleType &type) {
    return Append<arrow::DoubleScalar>();
  }

  arrow::Status Visit(const arrow::StringType &type) {
    return Append(std::dynamic_pointer_cast<arrow::StringScalar>(scalar));
  }

  arrow::Status Visit(const arrow::Date32Type &type) {
    return Append<arrow::Date32Scalar>();
  }

  arrow::Status Visit(const arrow::TimestampType &type) {
    return Append<arrow::TimestampScalar>();
  }

  arrow::Status Visit(const arrow::DurationType &type) {
    return Append<arrow::DurationScalar>();
  }

  template <typename T> arrow::Status Visit(const T &type) {
    return arrow::Status::NotImplemented(
        "AppendScalarTypeVisitor not implemented for " +
        builder.type()->ToString() + ": " + scalar->ToString());
  }

  arrow::ArrayBuilder &builder;
  std::shared_ptr<arrow::Scalar> scalar;
};

std::shared_ptr<arrow::Scalar> dispatchAggregationOperatorColumnVector(
    const Context &ctx, std::shared_ptr<arrow::Array> column,
    const PhysicalAggregation &aggregation) {
  arrow::compute::ExecContext context(ctx.memory_resource->converge());
  switch (aggregation.op) {
  case AggregationOperator::SUM:
    return ARA_GET_ARROW_RESULT(
               arrow::compute::Sum(
                   column, arrow::compute::ScalarAggregateOptions::Defaults(),
                   &context))
        .scalar();
  case AggregationOperator::MIN:
    return ARA_GET_ARROW_RESULT(
               arrow::compute::MinMax(
                   column, arrow::compute::ScalarAggregateOptions::Defaults(),
                   &context))
        .scalar_as<arrow::StructScalar>()
        .value[0];
  case AggregationOperator::MAX:
    return ARA_GET_ARROW_RESULT(
               arrow::compute::MinMax(
                   column, arrow::compute::ScalarAggregateOptions::Defaults(),
                   &context))
        .scalar_as<arrow::StructScalar>()
        .value[1];
  case AggregationOperator::COUNT_VALID:
    return ARA_GET_ARROW_RESULT(
               arrow::compute::Count(
                   column, arrow::compute::CountOptions::Defaults(), &context))
        .scalar();
  case AggregationOperator::COUNT_ALL:
    return std::make_shared<arrow::Int64Scalar>(column->length());
  case AggregationOperator::MEAN:
    return ARA_GET_ARROW_RESULT(
               arrow::compute::Mean(
                   column, arrow::compute::ScalarAggregateOptions::Defaults(),
                   &context))
        .scalar();
  case AggregationOperator::NTH_ELEMENT:
    if (column->length() == 0) {
      return Literal(aggregation.data_type.type_id).arrow();
    }
    return ARA_GET_ARROW_RESULT(column->GetScalar(aggregation.n));
  default:
    ARA_FAIL("Unsupported aggregation operator: " +
             aggregationOperatorToString(aggregation.op));
  }
}

std::shared_ptr<arrow::Scalar>
dispatchAggregationOperatorColumnScalar(std::shared_ptr<arrow::Scalar> scalar,
                                        size_t size, AggregationOperator op) {
  switch (op) {
  case AggregationOperator::COUNT_VALID:
    if (scalar->is_valid) {
      return std::make_shared<arrow::Int64Scalar>(size);
    } else {
      return std::make_shared<arrow::Int64Scalar>(0);
    }
  case AggregationOperator::COUNT_ALL:
    return std::make_shared<arrow::Int64Scalar>(size);
  default:
    ARA_FAIL("Unsupported aggregation operator: " +
             aggregationOperatorToString(op));
  }
}

std::shared_ptr<Fragment>
doAggregateWithoutKey(const Context &ctx, const Schema &schema,
                      const std::vector<PhysicalAggregation> &aggregations,
                      std::shared_ptr<const Fragment> fragment) {
  std::vector<std::shared_ptr<const Column>> results;
  for (const auto &aggregation : aggregations) {
    const auto &column = fragment->column<Column>(aggregation.idx);
    auto scalar = [&]() {
      if (auto cv = std::dynamic_pointer_cast<const ColumnVector>(column); cv) {
        return dispatchAggregationOperatorColumnVector(ctx, cv->arrow(),
                                                       aggregation);
      } else if (auto cs =
                     std::dynamic_pointer_cast<const ColumnScalar>(column);
                 cs) {
        return dispatchAggregationOperatorColumnScalar(cs->arrow(), cs->size(),
                                                       aggregation.op);
      } else {
        ARA_FAIL("Neither column vector nor column scalar");
      }
    }();
    auto casted = ARA_GET_ARROW_RESULT(
        arrow::compute::Cast(scalar, aggregation.data_type.arrow()));
    auto array =
        ARA_GET_ARROW_RESULT(arrow::MakeArrayFromScalar(*casted.scalar(), 1));
    results.emplace_back(createArrowColumnVector(aggregation.data_type, array));
  }
  return std::make_shared<Fragment>(std::move(results));
}

std::shared_ptr<Fragment>
doAggregate(const Context &ctx, const Schema &schema,
            const std::vector<ColumnIdx> &keys,
            const std::vector<PhysicalAggregation> &aggregations,
            std::shared_ptr<const Fragment> fragment) {
  /// Shortcut for non-key aggregations.
  if (keys.empty()) {
    return doAggregateWithoutKey(ctx, schema, aggregations, fragment);
  }

  auto pool = ctx.memory_resource->converge();
  arrow::compute::ExecContext context(pool);

  using Indices = std::pair<std::unique_ptr<arrow::ArrayBuilder>,
                            std::shared_ptr<arrow::Array>>;
  using IndicesHashTable = std::unordered_map<Key, Indices, RowHash, RowEqual>;
  IndicesHashTable indices;
  std::vector<std::shared_ptr<const Column>> results;

  /// Build hash table for group keys and record the grouped indices.
  {
    auto rows = fragment->size();
    for (size_t i = 0; i < rows; i++) {
      Key key_values;
      for (auto key : keys) {
        const auto &key_col = fragment->column(key);
        const auto &value =
            ARA_GET_ARROW_RESULT(key_col->arrow()->GetScalar(i));
        key_values.emplace_back(value);
      }
      auto it = indices.find(key_values);
      if (it == indices.end()) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        ARA_ASSERT_ARROW_OK(arrow::MakeBuilder(pool, arrow::uint64(), &builder),
                            "Create indices builder failed");
        std::tie(it, std::ignore) = indices.emplace(
            std::move(key_values), std::make_pair(std::move(builder), nullptr));
      }
      auto indices_builder =
          dynamic_cast<arrow::UInt64Builder *>(it->second.first.get());
      ARA_ASSERT_ARROW_OK(indices_builder->Append(i), "Append index failed");
    }
    for (auto &entry : indices) {
      ARA_ASSERT_ARROW_OK(entry.second.first->Finish(&entry.second.second),
                          "Finish indices failed");
    }
  }

  /// Gather group columns based on groups.
  {
    for (size_t i = 0; i < keys.size(); i++) {
      std::unique_ptr<arrow::ArrayBuilder> builder;
      ARA_ASSERT_ARROW_OK(arrow::MakeBuilder(pool, schema[i].arrow(), &builder),
                          "Create group builder failed");
      AppendScalarTypeVisitor visitor(*builder);
      for (const auto &entry : indices) {
        visitor.scalar = entry.first[i];
        ARA_ASSERT_ARROW_OK(
            arrow::VisitTypeInline(*entry.first[i]->type, &visitor),
            "Gather group scalar failed");
      }
      std::shared_ptr<arrow::Array> array;
      ARA_ASSERT_ARROW_OK(builder->Finish(&array), "Finish group failed");
      results.emplace_back(createArrowColumnVector(schema[i], array));
    }
  }

  /// Gather aggregation columns based on grouped indices.
  {
    for (const auto &aggregation : aggregations) {
      std::unique_ptr<arrow::ArrayBuilder> builder;
      ARA_ASSERT_ARROW_OK(
          arrow::MakeBuilder(pool, aggregation.data_type.arrow(), &builder),
          "Create aggregation builder failed");
      AppendScalarTypeVisitor visitor(*builder);
      for (const auto &entry : indices) {
        const auto &datum = ARA_GET_ARROW_RESULT(arrow::compute::Take(
            fragment->column(aggregation.idx)->arrow(), entry.second.second,
            arrow::compute::TakeOptions::Defaults(), &context));
        const auto &array = datum.make_array();
        auto scalar =
            dispatchAggregationOperatorColumnVector(ctx, array, aggregation);
        visitor.scalar = scalar;
        ARA_ASSERT_ARROW_OK(arrow::VisitTypeInline(*scalar->type, &visitor),
                            "Gather aggregation scalar failed");
      }
      std::shared_ptr<arrow::Array> array;
      ARA_ASSERT_ARROW_OK(builder->Finish(&array), "Finish aggregation failed");
      results.emplace_back(
          createArrowColumnVector(aggregation.data_type, array));
    }
  }

  return std::make_shared<Fragment>(std::move(results));
}

} // namespace ara::kernel::detail