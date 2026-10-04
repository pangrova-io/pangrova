#include "capability/CanonicalPhysicalTypes.h"
#include "CanonicalPhysicalTypeProjection.h"
#include "StructuralContractIdentities.h"

#include <stdexcept>

namespace pangrova::capability {
namespace {

using TypePtr = facebook::velox::TypePtr;
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
  return generated::projectCanonicalPhysicalType(type, canonicalPhysicalType);
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
  substrait::Type type;
  type.mutable_decimal()->set_precision(precision);
  type.mutable_decimal()->set_scale(scale);
  return canonicalPhysicalType(type);
}

TypePtr canonicalTimeType(int32_t precision) {
  substrait::Type type;
  type.mutable_precision_time()->set_precision(precision);
  return canonicalPhysicalType(type);
}

}  // namespace pangrova::capability
