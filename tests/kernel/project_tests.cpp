#include "ara/common/types.h"
#include "ara/expression/binary_op.h"
#include "ara/kernel/project.h"
#include "kernel_helper.h"
#include "utilities/data_helper.h"

#include <gtest/gtest.h>

using ara::VoidKernelId;
using ara::expression::BinaryOp;
using ara::expression::BinaryOperator;
using ara::expression::ColumnRef;
using ara::expression::Expression;
using ara::kernel::Project;
using ara::test::data::makeDirectColumnVector;
using ara::test::data::makeDirectColumnVectorN;
using ara::test::data::makeFragment;
using ara::type::DataType;
using ara::type::TypeId;

TEST(ProjectTest, ColumnRef) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto column_ref =
      std::make_shared<const ColumnRef>(1, DataType::int32Type(true));
  auto project = std::make_shared<const Project>(
      0, std::vector<std::shared_ptr<const Expression>>{column_ref});
  {
    auto cv_0 = makeDirectColumnVector<std::string>(DataType::stringType(true),
                                                    {"ab", ""}, {true, false});
    auto cv_1 = makeDirectColumnVector<int32_t>(DataType::int32Type(true),
                                                {0, 42}, {false, true});
    auto fragment = makeFragment(std::move(cv_0), std::move(cv_1));

    auto res_fragment = project->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 2);

    auto res_cvv = res_fragment->column(0);
    auto expected = makeDirectColumnVector<int32_t>(DataType::int32Type(true),
                                                    {0, 42}, {false, true});
    ARA_TEST_EXPECT_COLUMNS_EQUAL(expected, res_cvv);
  }
  {
    auto cv_0 =
        makeDirectColumnVector<std::string>(DataType::stringType(true), {}, {});
    auto fragment = makeFragment(std::move(cv_0));

    ASSERT_THROW(project->stream(ctx, 0, VoidKernelId, fragment, 0),
                 ara::LogicalError);
  }
}
