#pragma once

#include "ara/relational/rel_visitor.h"

#include <list>
#include <optional>
#include <unordered_map>

namespace ara::driver {
struct Option;
} // namespace ara::driver

namespace ara::execution {
class Pipeline;
} // namespace ara::execution

namespace ara::kernel {
struct HashJoinBuild;
struct Kernel;
struct NonStreamKernel;
struct Source;
} // namespace ara::kernel

namespace ara::planning {

using ara::driver::Option;
using ara::execution::Pipeline;
using ara::kernel::HashJoinBuild;
using ara::kernel::Kernel;
using ara::relational::Rel;
using ara::relational::RelAggregate;
using ara::relational::RelFilter;
using ara::relational::RelHashJoin;
using ara::relational::RelHashJoinBuild;
using ara::relational::RelHashJoinProbe;
using ara::relational::RelInputSource;
using ara::relational::RelLimit;
using ara::relational::RelProject;
using ara::relational::RelSort;
using ara::relational::RelUnion;
using ara::relational::RelUnionAll;
using ara::relational::RelVisitor;

namespace detail {

using ara::kernel::NonStreamKernel;
using ara::kernel::Source;

struct PipelineBuilder {
  std::vector<std::shared_ptr<const Source>> sources;
  std::vector<std::shared_ptr<const NonStreamKernel>> non_streams;

  PipelineBuilder() = default;

  PipelineBuilder(
      std::vector<std::shared_ptr<const Source>> sources_,
      std::vector<std::shared_ptr<const NonStreamKernel>> non_streams_);

  explicit PipelineBuilder(std::vector<PipelineBuilder> &&builders);
};

struct PipelineChain {
  std::list<PipelineBuilder> closed_pipelines;
  std::optional<PipelineBuilder> open_pipeline;
};

PipelineChain mergePipelineChains(std::vector<PipelineChain> &pipeline_chains);

} // namespace detail

/// Generate pipelines for the given Rel.
struct PipelineGenerator
    : RelVisitor<PipelineGenerator, std::shared_ptr<Kernel>> {
  explicit PipelineGenerator(const Option &option_) : option(option_) {}

  std::list<std::unique_ptr<Pipeline>>
  genPipelines(const std::shared_ptr<const Rel> &rel) &&;

public:
  std::shared_ptr<Kernel>
  visitInputSource(const std::shared_ptr<const RelInputSource> &input_source,
                   const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitFilter(const std::shared_ptr<const RelFilter> &filter,
              const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitUnion(const std::shared_ptr<const RelUnion> &u,
             const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitUnionAll(const std::shared_ptr<const RelUnionAll> &union_all,
                const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitHashJoin(const std::shared_ptr<const RelHashJoin> &hash_join,
                const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel> visitHashJoinBuild(
      const std::shared_ptr<const RelHashJoinBuild> &hash_join_build,
      const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel> visitHashJoinProbe(
      const std::shared_ptr<const RelHashJoinProbe> &hash_join_probe,
      const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitProject(const std::shared_ptr<const RelProject> &project,
               const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitAggregate(const std::shared_ptr<const RelAggregate> &aggregate,
                 const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitSort(const std::shared_ptr<const RelSort> &sort,
            const std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitLimit(const std::shared_ptr<const RelLimit> &sort,
             const std::vector<std::shared_ptr<Kernel>> &children);

private:
  template <typename KernelType, typename... Args>
  std::shared_ptr<KernelType> makeKernel(Args &&...args) {
    return std::make_shared<KernelType>(current_kernel_id++,
                                        std::forward<Args>(args)...);
  }

  std::shared_ptr<Kernel>
  combineResult(const std::shared_ptr<Kernel> &parent,
                const std::vector<std::shared_ptr<Kernel>> &children);

private:
  const Option &option;

private:
  KernelId current_kernel_id = 0;
  std::unordered_map<std::shared_ptr<const RelHashJoinBuild>,
                     std::shared_ptr<const HashJoinBuild>>
      hash_join_build_kernels;
  std::unordered_map<std::shared_ptr<Kernel>, detail::PipelineChain>
      pipeline_chains;
};

} // namespace ara::planning
