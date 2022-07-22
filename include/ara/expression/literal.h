#pragma once

#include "ara/expression/expressions.h"

#include <arrow/api.h>
#include <arrow/visitor.h>

namespace ara::expression {

using ara::type::TypeId;

namespace detail {

template <typename T, std::enable_if_t<!std::is_constructible<
                          std::decay_t<T>, std::string>::value> * = nullptr>
inline std::shared_ptr<arrow::Scalar>
createScalar(std::shared_ptr<arrow::DataType> data_type, T &&value) {
  return ARA_GET_ARROW_RESULT(
      arrow::MakeScalar(data_type, std::forward<T>(value)));
}

template <typename T, std::enable_if_t<std::is_constructible<
                          std::decay_t<T>, std::string>::value> * = nullptr>
inline std::shared_ptr<arrow::Scalar>
createScalar(std::shared_ptr<arrow::DataType> data_type, T &&value) {
  return arrow::MakeScalar(value);
}

template <typename T, typename Enable = void> struct BaseScalarVisitor {
  template <typename ScalarType,
            std::enable_if_t<!std::is_same_v<arrow::StringScalar, ScalarType>>
                * = nullptr>
  arrow::Status visit(const ScalarType &scalar) {
    value = scalar.value;
    return arrow::Status::OK();
  }

  template <typename ScalarType,
            std::enable_if_t<std::is_same_v<arrow::StringScalar, ScalarType>>
                * = nullptr>
  arrow::Status visit(const ScalarType &scalar) {
    value = scalar.value->ToString();
    return arrow::Status::OK();
  }

  T value;
};

template <typename T, typename Enable = void> struct GetValueScalarVisitor {
  template <typename ScalarType,
            std::enable_if_t<std::is_same_v<T, typename ScalarType::ValueType>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    value = scalar.value;
    return arrow::Status::OK();
  }

  template <typename ScalarType,
            std::enable_if_t<std::is_same_v<ScalarType, arrow::NullScalar>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    return arrow::Status::NotImplemented(
        "GetValueScalarVisitor not implemented for " + scalar.ToString());
  }

  template <typename ScalarType,
            std::enable_if_t<!std::is_same_v<T, typename ScalarType::ValueType>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    return arrow::Status::NotImplemented(
        "GetValueScalarVisitor not implemented for " + scalar.ToString());
  }

  T value;
};

template <typename T>
struct GetValueScalarVisitor<
    T, std::enable_if_t<std::is_constructible_v<T, std::string>>> {
  template <typename ScalarType,
            std::enable_if_t<std::is_same_v<arrow::StringScalar, ScalarType>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    value = scalar.value->ToString();
    return arrow::Status::OK();
  }

  template <typename ScalarType,
            std::enable_if_t<!std::is_same_v<arrow::StringScalar, ScalarType>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    return arrow::Status::NotImplemented(
        "GetValueScalarVisitor not implemented for " + scalar.ToString());
  }

  std::string value;
};

} // namespace detail

struct Literal : public Expression {
  explicit Literal(TypeId type_id)
      : data_type(type_id, true),
        scalar(arrow::MakeNullScalar(data_type.arrow())) {}

  template <typename T>
  explicit Literal(TypeId type_id, T &&value) : data_type(type_id, false) {
    scalar = detail::createScalar(data_type.arrow(), std::forward<T>(value));
    ARA_ASSERT(data_type.arrow()->Equals(scalar->type),
                "Mismatched ara and arrow data types " + data_type.toString() +
                    " vs " + DataType(scalar->type, false).toString());
  }

  const DataType &dataType() const override { return data_type; }

  std::shared_ptr<const Column>
  evaluate(const Context &ctx, ThreadId thread_id,
           const Fragment &fragment) const override;

  std::string name() const override { return "Literal"; }

  std::string toString() const override;

  template <typename T> T value() const {
    detail::GetValueScalarVisitor<T> visitor;
    ARA_ASSERT_ARROW_OK(arrow::VisitScalarInline(*scalar, &visitor),
                         "Get arrow value from literal failed")
    return visitor.value;
  }

  std::shared_ptr<arrow::Scalar> arrow() const { return scalar; }

private:
  DataType data_type;
  std::shared_ptr<arrow::Scalar> scalar;
};

} // namespace ara::expression
