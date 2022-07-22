#include "data_helper.h"

namespace cura::test::data {

namespace detail {

void assertArrowArraysEqual(const arrow::Array &expected,
                            const arrow::Array &actual, bool verbose) {
  std::stringstream diff;
  if (!expected.Equals(actual, arrow::EqualOptions().diff_sink(&diff))) {
    if (expected.data()->null_count != actual.data()->null_count) {
      diff << "Null counts differ. Expected " << expected.data()->null_count
           << " but was " << actual.data()->null_count << "\n";
    }
    if (verbose) {
      arrow::PrettyPrintOptions options(/*indent=*/2);
      options.window = 50;
      diff << "Expected:\n";
      CURA_ASSERT_ARROW_OK(PrettyPrint(expected, options, &diff), "");
      diff << "\nActual:\n";
      CURA_ASSERT_ARROW_OK(PrettyPrint(actual, options, &diff), "");
    }
    FAIL() << diff.str();
  }
}

} // namespace detail

void assertColumnsEqual(const ColumnVector &lhs, const ColumnVector &rhs,
                        bool sort) {
  auto left = lhs.arrow();
  auto right = rhs.arrow();

  if (sort) {
    auto left_indices =
        CURA_GET_ARROW_RESULT(arrow::compute::SortToIndices(*left));
    left = CURA_GET_ARROW_RESULT(arrow::compute::Take(*left, *left_indices));
    auto right_indices =
        CURA_GET_ARROW_RESULT(arrow::compute::SortToIndices(*right));
    right = CURA_GET_ARROW_RESULT(arrow::compute::Take(*right, *right_indices));
  }

  detail::assertArrowArraysEqual(*left, *right, true);
}

} // namespace cura::test::data
