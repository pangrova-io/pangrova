#include "engine/velox/connector/ParquetUuidReadPlan.h"

#include <stdexcept>
#include "velox/core/ExpressionEvaluator.h"
#include "velox/core/Expressions.h"
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/types/UuidType.h"

namespace pangrova::velox {
namespace {
using namespace facebook::velox;

bool containsUuid(const TypePtr& type) {
  if (isUuidType(type)) return true;
  for (uint32_t index = 0; index < type->size(); ++index) {
    if (containsUuid(type->childAt(index))) return true;
  }
  return false;
}

const parquet::ParquetTypeWithId* childNamed(
    const parquet::ParquetTypeWithId* source, const std::string& name) {
  if (!source) return nullptr;
  for (uint32_t index = 0; index < source->size(); ++index) {
    const auto& child = source->parquetChildAt(index);
    if (child.name_ == name) return &child;
  }
  return nullptr;
}

TypePtr physicalUuidReadType(const TypePtr& declared, const parquet::ParquetTypeWithId* source) {
  if (!containsUuid(declared)) return declared;
  if (isUuidType(declared)) {
    // An absent evolved field is a typed NULL. Present bytes require the
    // actual Parquet UUID annotation; plain binary(16) is never inferred.
    if (source && (source->parquetType_ != parquet::thrift::Type::FIXED_LEN_BYTE_ARRAY ||
        source->typeLength_ != 16 || !source->logicalType_ || !source->logicalType_->__isset.UUID)) {
      throw std::invalid_argument("Declared UUID requires an annotated Parquet fixed binary(16) field");
    }
    return VARBINARY();
  }
  if (declared->isRow()) {
    std::vector<TypePtr> fields;
    const auto& row = declared->asRow();
    for (uint32_t index = 0; index < row.size(); ++index) {
      fields.push_back(physicalUuidReadType(row.childAt(index), childNamed(source, row.nameOf(index))));
    }
    return ROW(row.names(), std::move(fields));
  }
  if (source && source->size() != declared->size()) {
    throw std::invalid_argument("Parquet UUID container differs from its declared schema");
  }
  auto child = [&](uint32_t index) {
    return physicalUuidReadType(declared->childAt(index), source ? &source->parquetChildAt(index) : nullptr);
  };
  if (declared->isArray()) return ARRAY(child(0));
  if (declared->isMap()) return MAP(child(0), child(1));
  throw std::invalid_argument("Unsupported Parquet UUID container");
}
}  // namespace

std::unique_ptr<ParquetUuidReadPlan> ParquetUuidReadPlan::create(
    const facebook::velox::RowTypePtr& logicalType,
    const facebook::velox::dwio::common::TypeWithId& fileType,
    facebook::velox::core::ExpressionEvaluator* evaluator) {
  if (!containsUuid(logicalType)) return nullptr;
  if (!evaluator) throw std::invalid_argument("Parquet UUID requires the task's Native expression evaluator");
  const auto* parquetType = dynamic_cast<const facebook::velox::parquet::ParquetTypeWithId*>(&fileType);
  if (!parquetType) throw std::invalid_argument("UUID file binding requires an actual Parquet schema");
  auto physical = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      physicalUuidReadType(logicalType, parquetType));
  return std::unique_ptr<ParquetUuidReadPlan>(new ParquetUuidReadPlan(
      std::move(physical), logicalType, evaluator));
}

ParquetUuidReadPlan::ParquetUuidReadPlan(facebook::velox::RowTypePtr physicalType,
    facebook::velox::RowTypePtr logicalType, facebook::velox::core::ExpressionEvaluator* evaluator)
    : physical_type_(std::move(physicalType)), logical_type_(std::move(logicalType)), evaluator_(evaluator) {
  std::vector<facebook::velox::core::TypedExprPtr> fields;
  for (uint32_t index = 0; index < logical_type_->size(); ++index) {
    auto input = std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
        physical_type_->childAt(index), physical_type_->nameOf(index));
    if (containsUuid(logical_type_->childAt(index))) {
      fields.push_back(std::make_shared<facebook::velox::core::CastTypedExpr>(
          logical_type_->childAt(index), input, false));
    } else {
      fields.push_back(std::move(input));
    }
  }
  expression_ = evaluator_->compile(fields);
}

ParquetUuidReadPlan::~ParquetUuidReadPlan() = default;

facebook::velox::RowVectorPtr ParquetUuidReadPlan::convert(const facebook::velox::RowVectorPtr& input) {
  if (!input || !input->type()->equivalent(*physical_type_)) {
    throw std::invalid_argument("Parquet UUID batch differs from its bound physical schema");
  }
  if (input->size() == 0) {
    return std::dynamic_pointer_cast<facebook::velox::RowVector>(
        facebook::velox::BaseVector::create(logical_type_, 0, input->pool()));
  }
  facebook::velox::SelectivityVector rows(input->size());
  std::vector<facebook::velox::VectorPtr> fields;
  evaluator_->evaluate(expression_.get(), rows, *input, fields);
  if (fields.size() != logical_type_->size()) {
    throw std::runtime_error("Native Parquet UUID cast changed the declared row width");
  }
  for (uint32_t index = 0; index < fields.size(); ++index) {
    if (!fields[index] || !fields[index]->type()->equivalent(*logical_type_->childAt(index))) {
      throw std::runtime_error("Native Parquet UUID cast changed a declared field type");
    }
  }
  return std::make_shared<facebook::velox::RowVector>(input->pool(), logical_type_,
      input->nulls(), input->size(), std::move(fields));
}

}  // namespace pangrova::velox
