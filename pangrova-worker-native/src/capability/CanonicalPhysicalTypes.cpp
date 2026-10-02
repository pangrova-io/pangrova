#include "capability/CanonicalPhysicalTypes.h"
#include "StructuralContractIdentities.h"

#include <stdexcept>
#include <unordered_map>

namespace pangrova::capability {
namespace {

using TypePtr = facebook::velox::TypePtr;
using Factory = TypePtr (*)(const substrait::Type&);

TypePtr positionalRow(const substrait::Type& type) {
  std::vector<TypePtr> children;
  for (int index = 0; index < type.struct_().types_size(); ++index) {
    children.push_back(canonicalPhysicalType(type.struct_().types(index)));
  }
  return canonicalComputedRowType(std::move(children));
}

const std::unordered_map<int, Factory>& factories() {
  using namespace facebook::velox;
  static const std::unordered_map<int, Factory> values = {
      {substrait::Type::kBool, [](const auto&) -> TypePtr { return BOOLEAN(); }},
      {substrait::Type::kI8, [](const auto&) -> TypePtr { return TINYINT(); }},
      {substrait::Type::kI16, [](const auto&) -> TypePtr { return SMALLINT(); }},
      {substrait::Type::kI32, [](const auto&) -> TypePtr { return INTEGER(); }},
      {substrait::Type::kI64, [](const auto&) -> TypePtr { return BIGINT(); }},
      {substrait::Type::kFp32, [](const auto&) -> TypePtr { return REAL(); }},
      {substrait::Type::kFp64, [](const auto&) -> TypePtr { return DOUBLE(); }},
      {substrait::Type::kString, [](const auto&) -> TypePtr { return VARCHAR(); }},
      {substrait::Type::kVarchar, [](const auto& type) -> TypePtr {
         if (type.varchar().length() <= 0) throw std::runtime_error("VARCHAR length must be positive");
         return VARCHAR();
       }},
      {substrait::Type::kFixedChar, [](const auto& type) -> TypePtr {
         if (type.fixed_char().length() <= 0) throw std::runtime_error("CHAR length must be positive");
         return VARCHAR();
       }},
      {substrait::Type::kBinary, [](const auto&) -> TypePtr { return VARBINARY(); }},
      {substrait::Type::kDate, [](const auto&) -> TypePtr { return DATE(); }},
      {substrait::Type::kPrecisionTimestamp, [](const auto&) -> TypePtr { return TIMESTAMP(); }},
      {substrait::Type::kPrecisionTimestampTz, [](const auto&) -> TypePtr { return TIMESTAMP(); }},
      {substrait::Type::kPrecisionTime, [](const auto& type) -> TypePtr {
         return canonicalTimeType(type.precision_time().precision());
       }},
      {substrait::Type::kStruct, positionalRow},
      {substrait::Type::kDecimal, [](const auto& type) -> TypePtr {
         return canonicalDecimalType(type.decimal().precision(), type.decimal().scale());
       }},
      {substrait::Type::kList, [](const auto& type) -> TypePtr {
         return ARRAY(canonicalPhysicalType(type.list().type()));
       }},
      {substrait::Type::kMap, [](const auto& type) -> TypePtr {
         return MAP(canonicalPhysicalType(type.map().key()), canonicalPhysicalType(type.map().value()));
       }},
      {substrait::Type::kIntervalYear, [](const auto&) -> TypePtr { return INTERVAL_YEAR_MONTH(); }},
      {substrait::Type::kIntervalDay, [](const auto&) -> TypePtr { return INTERVAL_DAY_TIME(); }}};
  return values;
}

TypePtr consumingNames(
    const substrait::Type& type,
    const google::protobuf::RepeatedPtrField<std::string>& names, int& ordinal) {
  if (type.has_struct_()) {
    std::vector<std::string> childrenNames;
    std::vector<TypePtr> children;
    for (const auto& child : type.struct_().types()) {
      if (ordinal >= names.size()) {
        throw std::runtime_error("Canonical NamedStruct is missing DFS field names");
      }
      childrenNames.push_back(names.Get(ordinal++));
      children.push_back(consumingNames(child, names, ordinal));
    }
    return facebook::velox::ROW(std::move(childrenNames), std::move(children));
  }
  if (type.has_list()) {
    return facebook::velox::ARRAY(consumingNames(type.list().type(), names, ordinal));
  }
  if (type.has_map()) {
    auto key = consumingNames(type.map().key(), names, ordinal);
    auto value = consumingNames(type.map().value(), names, ordinal);
    return facebook::velox::MAP(std::move(key), std::move(value));
  }
  return canonicalPhysicalType(type);
}

}  // namespace

TypePtr canonicalPhysicalType(const substrait::Type& type) {
  const auto factory = factories().find(type.kind_case());
  if (factory == factories().end()) {
    throw std::runtime_error("Unsupported canonical physical type kind: " +
                             std::to_string(static_cast<int>(type.kind_case())));
  }
  return factory->second(type);
}

facebook::velox::RowTypePtr canonicalPhysicalSchema(const substrait::NamedStruct& schema) {
  if (!schema.has_struct_()) {
    throw std::runtime_error("Canonical NamedStruct requires an explicit struct");
  }
  substrait::Type type;
  *type.mutable_struct_() = schema.struct_();
  int ordinal = 0;
  auto result = consumingNames(type, schema.names(), ordinal);
  if (ordinal != schema.names_size()) {
    throw std::runtime_error("Canonical NamedStruct has unused DFS field names");
  }
  return std::dynamic_pointer_cast<const facebook::velox::RowType>(result);
}

facebook::velox::RowTypePtr canonicalComputedRowType(std::vector<TypePtr> children) {
  std::vector<std::string> names;
  names.reserve(children.size());
  for (size_t ordinal = 0; ordinal < children.size(); ++ordinal) {
    names.push_back(std::string(generated::anonymousComputedFieldPrefix) + std::to_string(ordinal));
  }
  return facebook::velox::ROW(std::move(names), std::move(children));
}

TypePtr canonicalDecimalType(int32_t precision, int32_t scale) {
  if (precision < 1 || precision > 38 || scale < 0 || scale > precision) {
    throw std::runtime_error(
        "Substrait DECIMAL requires precision in [1, 38] and scale in [0, precision]");
  }
  return facebook::velox::DECIMAL(static_cast<uint8_t>(precision), static_cast<uint8_t>(scale));
}

TypePtr canonicalTimeType(int32_t precision) {
  if (precision < 0 || precision > 6) {
    throw std::runtime_error("Substrait time precision must be in [0, 6]");
  }
  return precision <= 3 ? TypePtr(facebook::velox::TIME())
                        : TypePtr(facebook::velox::TIME_MICRO_UTC());
}

}  // namespace pangrova::capability
