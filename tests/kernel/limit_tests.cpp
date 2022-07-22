#include "ara/common/types.h"
#include "ara/kernel/limit.h"
#include "ara/kernel/sources.h"
#include "kernel_helper.h"
#include "utilities/data_helper.h"

#include <gtest/gtest.h>

using ara::VoidKernelId;
using ara::kernel::Limit;
using ara::test::data::makeDirectColumnVectorN;
using ara::test::data::makeFragment;
using ara::type::DataType;
using ara::type::TypeId;

TEST(LimitTest, SingleFragmentLess) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto limit = std::make_shared<const Limit>(0, 1, 7);
  {
    auto cv = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 4, 0);
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = limit->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 3);
  }
}

TEST(LimitTest, SingleFragmentMore) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto limit = std::make_shared<const Limit>(0, 1, 7);
  {
    auto cv = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 42, 0);
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = limit->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 7);
  }
}

TEST(LimitTest, TwoFragmentsEmptyThenMore) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto limit = std::make_shared<const Limit>(0, 4, 7);
  {
    auto cv = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 3, 0);
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = limit->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_EQ(res_fragment, nullptr);
  }
  {
    auto cv = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 42, 0);
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = limit->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 7);
  }
}

TEST(LimitTest, TwoFragmentsEmptyThenLess) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto limit = std::make_shared<const Limit>(0, 4, 7);
  {
    auto cv = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 3, 0);
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = limit->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_EQ(res_fragment, nullptr);
  }
  {
    auto cv = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 4, 0);
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = limit->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 3);
  }
}

TEST(LimitTest, TwoFragmentsLessThenMore) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto limit = std::make_shared<const Limit>(0, 4, 7);
  {
    auto cv = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 5, 0);
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = limit->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 1);
  }
  {
    auto cv = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 42, 0);
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = limit->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 6);
  }
}
