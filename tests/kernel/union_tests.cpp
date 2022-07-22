#include "ara/kernel/sources.h"
#include "ara/kernel/unions.h"
#include "kernel_helper.h"
#include "utilities/data_helper.h"

#include <gtest/gtest.h>

using ara::VoidKernelId;
using ara::VoidThreadId;
using ara::kernel::HeapSource;
using ara::kernel::Union;
using ara::test::data::assertColumnsEqual;
using ara::test::data::makeDirectColumnVector;
using ara::test::data::makeFragment;
using ara::type::DataType;
using ara::type::Schema;

TEST(UnionTest, Union) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto u = std::make_shared<const Union>(
      0, Schema{DataType::int64Type(true), DataType::stringType(true)});

  auto cv0 = makeDirectColumnVector<int64_t>(
      DataType::int64Type(true), {41, 41, 42, 42, 0, 0},
      {true, true, true, true, false, false});
  auto cv1 = makeDirectColumnVector<std::string>(
      DataType::stringType(true), {"ab", "", "cd", "cd", "", ""},
      {true, false, true, true, false, false});
  auto fragment0 = makeFragment(std::move(cv0), std::move(cv1));

  u->push(ctx, VoidThreadId, VoidKernelId, fragment0);
  u->concatenate(ctx);
  u->converge(ctx);

  auto heap_source = std::make_shared<HeapSource>(1, u);
  auto res = heap_source->stream(ctx, 0, VoidKernelId, nullptr, 1024);

  ASSERT_NE(res, nullptr);
  ASSERT_EQ(res->numColumns(), 2);
  ASSERT_EQ(res->size(), 4);

  auto expected0 = makeDirectColumnVector<int64_t>(
      DataType::int64Type(true), {41, 41, 42, 0}, {true, true, true, false});
  auto expected1 = makeDirectColumnVector<std::string>(
      DataType::stringType(true), {"ab", "", "cd", ""},
      {true, false, true, false});
  ARA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
  ARA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
}
