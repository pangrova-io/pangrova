#include "velox/plan/CanonicalPlanSchema.h"

#include <algorithm>
#include <stdexcept>

#include "StructuralContractIdentities.h"
#include "capability/CanonicalPhysicalTypes.h"

namespace pangrova::velox::schema {
namespace {
size_t nestedNames(const substrait::Type& type) {
  size_t count = 0;
  if (type.has_struct_()) {
    for (const auto& child : type.struct_().types()) count += 1 + nestedNames(child);
  } else if (type.has_list()) count = nestedNames(type.list().type());
  else if (type.has_map()) count = nestedNames(type.map().key()) + nestedNames(type.map().value());
  return count;
}

void anonymous(const substrait::Type& type, std::vector<std::string>& names) {
  if (type.has_struct_()) {
    for (int i = 0; i < type.struct_().types_size(); ++i) {
      names.push_back(std::string(pangrova::capability::generated::anonymousComputedFieldPrefix) + std::to_string(i));
      anonymous(type.struct_().types(i), names);
    }
  } else if (type.has_list()) anonymous(type.list().type(), names);
  else if (type.has_map()) { anonymous(type.map().key(), names); anonymous(type.map().value(), names); }
}

google::protobuf::Message* body(substrait::Type& type) {
  const auto field = type.GetDescriptor()->FindFieldByNumber(type.kind_case());
  if (!field || field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
    throw std::invalid_argument("Canonical type has no declared kind");
  }
  return type.GetReflection()->MutableMessage(&type, field);
}

substrait::Type eraseNullability(substrait::Type type) {
  type = withNullability(std::move(type), substrait::Type::NULLABILITY_REQUIRED);
  if (type.has_struct_()) {
    for (auto& child : *type.mutable_struct_()->mutable_types()) child = eraseNullability(std::move(child));
  } else if (type.has_list()) *type.mutable_list()->mutable_type() = eraseNullability(type.list().type());
  else if (type.has_map()) {
    *type.mutable_map()->mutable_key() = eraseNullability(type.map().key());
    *type.mutable_map()->mutable_value() = eraseNullability(type.map().value());
  }
  return type;
}
}  // namespace

std::vector<Field> fields(const substrait::NamedStruct& input) {
  // The existing physical bridge is also the strict DFS/type validation owner.
  pangrova::capability::canonicalPhysicalSchema(input);
  std::vector<Field> result;
  size_t cursor = 0;
  for (const auto& type : input.struct_().types()) {
    validateValueType(type);
    const auto count = nestedNames(type);
    if (cursor + 1 + count > static_cast<size_t>(input.names_size())) {
      throw std::invalid_argument("Canonical schema is missing DFS names");
    }
    Field field{input.names(cursor++), type, {}};
    for (size_t i = 0; i < count; ++i) field.descendants.push_back(input.names(cursor++));
    result.push_back(std::move(field));
  }
  if (cursor != static_cast<size_t>(input.names_size())) throw std::invalid_argument("Canonical schema has extra DFS names");
  return result;
}

substrait::NamedStruct named(const std::vector<Field>& source, const substrait::Type_Struct* root) {
  substrait::NamedStruct result;
  if (root) *result.mutable_struct_() = *root;
  else result.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  result.mutable_struct_()->clear_types();
  for (const auto& field : source) {
    result.add_names(field.name);
    for (const auto& name : field.descendants) result.add_names(name);
    *result.mutable_struct_()->add_types() = field.type;
  }
  fields(result);
  return result;
}

Field computed(std::string name, substrait::Type type) {
  validateValueType(type);
  Field result{std::move(name), std::move(type), {}};
  anonymous(result.type, result.descendants);
  return result;
}

substrait::Type withNullability(substrait::Type type, substrait::Type::Nullability nullability) {
  auto value = body(type);
  const auto field = value->GetDescriptor()->FindFieldByName("nullability");
  if (!field) throw std::invalid_argument("Canonical type has no nullability contract");
  value->GetReflection()->SetEnumValue(value, field, nullability);
  return type;
}

bool nullable(const substrait::Type& type) {
  auto copy = type;
  const auto value = body(copy);
  const auto field = value->GetDescriptor()->FindFieldByName("nullability");
  const auto result = field ? value->GetReflection()->GetEnumValue(*value, field) : 0;
  if (result != substrait::Type::NULLABILITY_REQUIRED && result != substrait::Type::NULLABILITY_NULLABLE) {
    throw std::invalid_argument("Canonical value type requires explicit nullability");
  }
  return result == substrait::Type::NULLABILITY_NULLABLE;
}

substrait::Type primitive(const std::string& kind, substrait::Type::Nullability nullability) {
  substrait::Type result;
  const auto field = result.GetDescriptor()->FindFieldByName(kind);
  if (!field || !field->containing_oneof()) throw std::invalid_argument("Unknown canonical type kind: " + kind);
  result.GetReflection()->MutableMessage(&result, field);
  return withNullability(std::move(result), nullability);
}

substrait::Type merge(const std::vector<substrait::Type>& types) {
  if (types.empty()) throw std::invalid_argument("Empty expression requires an explicit declared type");
  for (const auto& type : types) validateValueType(type);
  auto result = types.front();
  const auto physical = eraseNullability(result).SerializeAsString();
  for (const auto& type : types) {
    if (eraseNullability(type).SerializeAsString() != physical) {
      throw std::invalid_argument("Canonical expression types differ in kind, shape or parameters");
    }
  }
  if (result.has_struct_()) {
    for (int i = 0; i < result.struct_().types_size(); ++i) {
      std::vector<substrait::Type> children;
      for (const auto& type : types) children.push_back(type.struct_().types(i));
      *result.mutable_struct_()->mutable_types(i) = merge(children);
    }
  } else if (result.has_list()) {
    std::vector<substrait::Type> children;
    for (const auto& type : types) children.push_back(type.list().type());
    *result.mutable_list()->mutable_type() = merge(children);
  } else if (result.has_map()) {
    std::vector<substrait::Type> keys, values;
    for (const auto& type : types) { keys.push_back(type.map().key()); values.push_back(type.map().value()); }
    *result.mutable_map()->mutable_key() = merge(keys);
    *result.mutable_map()->mutable_value() = merge(values);
  }
  const bool allowsNull = std::any_of(types.begin(), types.end(), nullable);
  if (!allowsNull && !result.has_struct_() && !result.has_list() && !result.has_map()) return result;
  return withNullability(std::move(result), allowsNull
      ? substrait::Type::NULLABILITY_NULLABLE : substrait::Type::NULLABILITY_REQUIRED);
}

substrait::NamedStruct emit(const substrait::NamedStruct& input, const substrait::RelCommon& common) {
  if (!common.has_emit()) return input;
  const auto source = fields(input);
  std::vector<Field> selected;
  for (const auto ordinal : common.emit().output_mapping()) {
    if (ordinal < 0 || static_cast<size_t>(ordinal) >= source.size()) throw std::invalid_argument("Emit ordinal is out of bounds");
    selected.push_back(source[ordinal]);
  }
  return named(selected, &input.struct_());
}
}  // namespace pangrova::velox::schema
