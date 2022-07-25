#pragma onec

#include "ara/data/column_factories.h"
#include "ara/data/column_scalar.h"
#include "ara/data/column_vector.h"
#include "ara/data/fragment.h"
#include "ara/expression/literal.h"
#include "utilities/database/table.h"

#include <arrow/compute/api.h>
#include <arrow/visitor.h>
#include <gtest/gtest.h>
#include <sstream>

namespace ara::test::data {

using ara::data::Column;
using ara::data::ColumnScalar;
using ara::data::ColumnVector;
using ara::data::createArrowColumnScalar;
using ara::data::createArrowColumnVector;
using ara::data::Fragment;
using ara::expression::Literal;
using ara::test::database::Table;
using ara::test::database::TableId;
using ara::type::DataType;

namespace detail {

template <template <typename...> typename Container, typename T>
struct BaseTypeVisitor : public arrow::TypeVisitor {
  explicit BaseTypeVisitor(DataType data_type_, Container<T> &&container_,
                           std::vector<bool> valid_mask_)
      : data_type(std::move(data_type_)), container(std::move(container_)),
        valid_mask(std::move(valid_mask_)) {}

  template <
      typename ArrowType,
      typename BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType>
  arrow::Status visit(const ArrowType &type) {
    auto pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::ArrayBuilder> builder;
    ARA_ASSERT_ARROW_OK(arrow::MakeBuilder(pool, data_type.arrow(), &builder),
                        "Creating arrow column builder failed");
    auto type_builder = dynamic_cast<BuilderType *>(builder.get());
    ARA_ASSERT(type_builder, "Cast to concrete builder failed");
    size_t i_valid_mask = 0;
    for (const auto &v : container) {
      if (!data_type.nullable || valid_mask[i_valid_mask]) {
        ARA_ASSERT_ARROW_OK(type_builder->Append(v),
                            "Append to arrow array failed");
      } else {
        ARA_ASSERT_ARROW_OK(type_builder->AppendNull(),
                            "Append null to arrow array failed");
      }
      i_valid_mask++;
    }
    std::shared_ptr<arrow::Array> array;
    ARA_ASSERT_ARROW_OK(type_builder->Finish(&array),
                        "Finish arrow build failed");

    cv = createArrowColumnVector(data_type, array);
    return arrow::Status::OK();
  }

  DataType data_type;
  Container<T> container;
  std::vector<bool> valid_mask;

  std::unique_ptr<ColumnVector> cv;
};

template <template <typename...> typename Container, typename T,
          typename ArrowType, typename Enable = void>
struct MakeArrowTypeVisitor : public BaseTypeVisitor<Container, T> {
  using BaseTypeVisitor<Container, T>::BaseTypeVisitor;
};

template <template <typename...> typename Container, typename T,
          typename ArrowType>
struct MakeArrowTypeVisitor<
    Container, T, ArrowType,
    std::enable_if_t<
        std::is_same_v<arrow::StringType, ArrowType> &&
        std::is_same_v<typename arrow::CTypeTraits<T>::ArrowType, ArrowType>>>
    : public BaseTypeVisitor<Container, T> {
  using BaseTypeVisitor<Container, T>::BaseTypeVisitor;

  arrow::Status Visit(const ArrowType &type) override {
    return BaseTypeVisitor<Container, T>::visit(type);
  }
};

template <template <typename...> typename Container, typename T,
          typename ArrowType>
struct MakeArrowTypeVisitor<
    Container, T, ArrowType,
    std::enable_if_t<std::is_same_v<typename ArrowType::c_type, T>>>
    : public BaseTypeVisitor<Container, T> {
  using BaseTypeVisitor<Container, T>::BaseTypeVisitor;

  arrow::Status Visit(const ArrowType &type) override {
    return BaseTypeVisitor<Container, T>::visit(type);
  }
};

} // namespace detail

template <typename T, template <typename...> typename Container = std::vector>
inline std::unique_ptr<ColumnVector>
makeArrowColumnVector(const DataType &data_type, Container<T> &&data,
                      std::vector<bool> valid_mask = {});

template <typename T>
inline std::unique_ptr<const ColumnVector>
makeArrowColumnVectorN(const DataType &data_type, size_t n, T start = 0);

template <typename T, template <typename...> typename Container>
inline std::unique_ptr<ColumnVector>
makeArrowColumnVector(const DataType &data_type, Container<T> &&data,
                      std::vector<bool> valid_mask) {
  if (data_type.nullable) {
    ARA_ASSERT(data.size() == valid_mask.size(),
               "Mismatched sizes between data and valid mask");
  }

  std::unique_ptr<detail::BaseTypeVisitor<Container, T>> visitor;

#define MAKE_TYPE_VISITOR(TYPE_CLASS)                                          \
  case arrow::TYPE_CLASS##Type::type_id:                                       \
    visitor = std::make_unique<                                                \
        detail::MakeArrowTypeVisitor<Container, T, arrow::TYPE_CLASS##Type>>(  \
        data_type, std::forward<Container<T>>(data), std::move(valid_mask));   \
    break;

  switch (data_type.arrow()->id()) {
    ARROW_GENERATE_FOR_ALL_TYPES(MAKE_TYPE_VISITOR)
  default:
    ARA_FAIL("Unsupported");
  }
#undef MAKE_TYPE_VISITOR

  ARA_ASSERT_ARROW_OK(data_type.arrow()->Accept(visitor.get()),
                      "Create arrow array failed");
  return std::move(visitor->cv);
}

template <typename T>
inline std::unique_ptr<const ColumnVector>
makeArrowColumnVectorN(const DataType &data_type, size_t n, T start) {
  std::vector<T> data(n);
  std::iota(data.begin(), data.end(), start);
  return makeArrowColumnVector(data_type, std::move(data));
}

template <typename T, template <typename...> typename Container = std::vector>
inline std::unique_ptr<ColumnVector>
makeDirectColumnVector(const DataType &data_type, Container<T> &&data,
                       std::vector<bool> valid_mask = {}) {
  return makeArrowColumnVector(data_type, std::forward<Container<T>>(data),
                               std::move(valid_mask));
}

template <typename T>
inline std::unique_ptr<const ColumnVector>
makeDirectColumnVectorN(const DataType &data_type, size_t n, T start = 0) {
  return makeArrowColumnVectorN(data_type, n, start);
}

template <typename T>
inline std::unique_ptr<const ColumnScalar>
makeDirectColumnScalar(const DataType &data_type, T &&value, size_t size) {
  auto literal = [&]() {
    return data_type.nullable
               ? Literal(data_type.type_id)
               : Literal(data_type.type_id, std::forward<T>(value));
  }();
  return createArrowColumnScalar(data_type, size, literal.arrow());
}

template <typename... ColumnVectors>
inline std::shared_ptr<const Fragment> makeFragment(ColumnVectors &&...cvs) {
  std::vector<std::shared_ptr<const Column>> vc;
  vc.reserve(sizeof...(ColumnVectors));
  (vc.emplace_back(std::forward<ColumnVectors>(cvs)), ...);
  return std::make_shared<Fragment>(std::move(vc));
}

void assertColumnsEqual(const ColumnVector &lhs, const ColumnVector &rhs,
                        bool sort = false);

#define ARA_TEST_EXPECT_COLUMNS_EQUAL(lhs, rhs...)                             \
  ara::test::data::assertColumnsEqual(*lhs, *rhs)

#define ARA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(lhs, rhs...)                     \
  ara::test::data::assertColumnsEqual(*lhs, *rhs, true)

} // namespace ara::test::data
