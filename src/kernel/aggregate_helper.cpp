#include "aggregate_helper.h"
#include "ara/data/column_scalar.h"
#include "ara/data/column_vector.h"
#include "ara/data/fragment.h"
#include "ara/expression/literal.h"
#include "helper.h"

#include <arrow/compute/api.h>
#include <arrow/visitor.h>
#include <sstream>

#include <arrow/util/checked_cast.h>

namespace ara::kernel::detail {

using ara::data::Column;
using ara::data::ColumnScalar;
using ara::data::ColumnVector;
using ara::expression::Literal;

// TODO: Rewrite these shit in a mature arrow fashion, i.e., array data visitors
// or so.
using ara::data::createArrowColumnVector;

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

arrow::compute::Aggregate toArrowAggregate(const PhysicalAggregation &agg) {
  switch (agg.op) {
    case AggregationOperator::SUM: return {"hash_sum"};
    case AggregationOperator::PRODUCT: return {"hash_product"};
    case AggregationOperator::MIN: return {"hash_min"};
    case AggregationOperator::MAX: return {"hash_max"};
    case AggregationOperator::COUNT_VALID: return {"hash_count", std::make_shared<arrow::compute::CountOptions>(arrow::compute::CountOptions::ALL)};
    case AggregationOperator::COUNT_ALL: return {"hash_count", std::make_shared<arrow::compute::CountOptions>(arrow::compute::CountOptions::ONLY_VALID)};
    case AggregationOperator::ANY:
    case AggregationOperator::ALL:
    case AggregationOperator::SUM_OF_SQUARES:
    case AggregationOperator::MEAN:
    case AggregationOperator::MEDIAN:
    case AggregationOperator::QUANTILE:
    case AggregationOperator::ARGMAX:
    case AggregationOperator::ARGMIN:
    case AggregationOperator::NUNIQUE:
    case AggregationOperator::NTH_ELEMENT: return {};
  }
}

arrow::Result<std::vector<const arrow::compute::HashAggregateKernel*>> GetKernels(
    arrow::compute::ExecContext* ctx, const std::vector<arrow::compute::Aggregate>& aggregates,
    const std::vector<arrow::TypeHolder>& in_types) {
  if (aggregates.size() != in_types.size()) {
    return arrow::Status::Invalid(aggregates.size(), " aggregate functions were specified but ",
                           in_types.size(), " arguments were provided.");
  }

  std::vector<const arrow::compute::HashAggregateKernel*> kernels(in_types.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto function,
                          ctx->func_registry()->GetFunction(aggregates[i].function));
    ARROW_ASSIGN_OR_RAISE(const arrow::compute::Kernel* kernel,
                          function->DispatchExact({in_types[i], arrow::uint32()}));
    kernels[i] = static_cast<const arrow::compute::HashAggregateKernel*>(kernel);
  }
  return kernels;
}

arrow::Result<std::vector<std::unique_ptr<arrow::compute::KernelState>>> InitKernels(
    const std::vector<const arrow::compute::HashAggregateKernel*>& kernels, arrow::compute::ExecContext* ctx,
    const std::vector<arrow::compute::Aggregate>& aggregates, const std::vector<arrow::TypeHolder>& in_types) {
  std::vector<std::unique_ptr<arrow::compute::KernelState>> states(kernels.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    const arrow::compute::FunctionOptions* options =
        arrow::internal::checked_cast<const arrow::compute::FunctionOptions*>(
            aggregates[i].options.get());

    if (options == nullptr) {
      // use known default options for the named function if possible
      auto maybe_function = ctx->func_registry()->GetFunction(aggregates[i].function);
      if (maybe_function.ok()) {
        options = maybe_function.ValueOrDie()->default_options();
      }
    }

    arrow::compute::KernelContext kernel_ctx{ctx};
    ARROW_ASSIGN_OR_RAISE(states[i],
                          kernels[i]->init(&kernel_ctx, arrow::compute::KernelInitArgs{kernels[i],
                                                                                       {
                                                                                           in_types[i],
                                                                                           arrow::uint32(),
                                                                                       },
                                                                                       options}));
  }

  return std::move(states);
}

arrow::Result<arrow::FieldVector> ResolveKernels(
    const std::vector<arrow::compute::Aggregate>& aggregates,
    const std::vector<const arrow::compute::HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<arrow::compute::KernelState>>& states, arrow::compute::ExecContext* ctx,
    const std::vector<arrow::TypeHolder>& types) {
  arrow::FieldVector fields(types.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    arrow::compute::KernelContext kernel_ctx{ctx};
    kernel_ctx.SetState(states[i].get());

    ARROW_ASSIGN_OR_RAISE(auto type, kernels[i]->signature->out_type().Resolve(
        &kernel_ctx, {types[i], arrow::uint32()}));
    fields[i] = field(aggregates[i].function, type.GetSharedPtr());
  }
  return fields;
}

arrow::Result<arrow::compute::ExecBatch> toExecBatch(std::shared_ptr<const Fragment> fragment, const std::vector<ColumnIdx> &indices) {
  std::vector<arrow::Datum> cols;
  cols.reserve(indices.size());
  for (auto idx : indices) {
    cols.emplace_back(fragment->column(idx)->arrow());
  }
  return arrow::compute::ExecBatch::Make(std::move(cols));
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

  std::vector<ColumnIdx> agg_cols(aggregations.size());
  std::transform(
      aggregations.begin(), aggregations.end(), agg_cols.begin(),
      [](const auto &agg) { return agg.idx; });

  std::vector<arrow::compute::Aggregate> aggs(aggregations.size());
  std::transform(aggregations.begin(), aggregations.end(), aggs.begin(), [](const auto &agg) { return toArrowAggregate(agg); });

  const auto &agg_eb = ARA_GET_ARROW_RESULT(toExecBatch(fragment, agg_cols));
  const auto agg_types = agg_eb.GetTypes();

  const auto &kernels = ARA_GET_ARROW_RESULT(GetKernels(&context, aggs, agg_types));

  const auto &states = ARA_GET_ARROW_RESULT(InitKernels(kernels, &context, aggs, agg_types));

  const auto &out_fields = ARA_GET_ARROW_RESULT(ResolveKernels(aggs, kernels, states, &context, agg_types));

  const auto &key_eb = ARA_GET_ARROW_RESULT(toExecBatch(fragment, keys));
  const auto key_types = key_eb.GetTypes();

  const auto &grouper = ARA_GET_ARROW_RESULT(arrow::compute::Grouper::Make(key_types));

  const auto id_batch = grouper->Consume(arrow::compute::ExecSpan{key_eb});
  // consume group ids with HashAggregateKernels
  for (size_t i = 0; i < kernels.size(); ++i) {
    arrow::compute::KernelContext batch_ctx{&context};
    batch_ctx.SetState(states[i].get());
    arrow::compute::ExecSpan agg_es(agg_eb);
    arrow::compute::ExecSpan kernel_batch({agg_es[i], *id_batch->array()}, agg_es.length);
    ARA_ASSERT_ARROW_OK(kernels[i]->resize(&batch_ctx, grouper->num_groups()), "Aggregate kernel resize failed");
    ARA_ASSERT_ARROW_OK(kernels[i]->consume(&batch_ctx, kernel_batch), "Aggregate kernel consume failed");
  }

  // Finalize output
  arrow::ArrayDataVector out_data(aggregations.size() + keys.size());
  auto it = out_data.begin();

  const auto &out_keys = ARA_GET_ARROW_RESULT(grouper->GetUniques());
  for (const auto& key : out_keys.values) {
    *it++ = key.array();
  }

  for (size_t idx = 0; idx < kernels.size(); ++idx) {
    arrow::compute::KernelContext batch_ctx{&context};
    batch_ctx.SetState(states[idx].get());
    arrow::Datum out;
    ARA_ASSERT_ARROW_OK(kernels[idx]->finalize(&batch_ctx, &out), "Aggregate kernel finalize failed");
    *it++ = out.array();
  }

  int64_t length = out_data[0]->length;

  arrow::SchemaBuilder builder;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (size_t i = 0; i < schema.size(); i++) {
    const auto &data_type = schema[i];
    auto field = std::make_shared<arrow::Field>("", data_type.arrow(),
                                                data_type.nullable);
    ARA_ASSERT_ARROW_OK(builder.AddField(field), "Add arrow field failed");
  }
  auto arrow_schema = ARA_GET_ARROW_RESULT(builder.Finish());

  auto rb = arrow::RecordBatch::Make(arrow_schema, length, std::move(out_data));
  return std::make_shared<Fragment>(std::move(rb));
}

} // namespace ara::kernel::detail