#include "capability/RegisteredTypeTemplate.h"

#include "CanonicalPhysicalTypeProjection.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace pangrova::capability {
namespace {
using namespace protocol::v2;
using TypeSignature = facebook::velox::exec::TypeSignature;
using Signature = facebook::velox::exec::FunctionSignature;
using Message = google::protobuf::Message;
using Field = google::protobuf::FieldDescriptor;

std::string upper(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char character) {
    return static_cast<char>(std::toupper(character));
  });
  return value;
}

FunctionOpaqueRegisteredType opaque(const TypeSignature& source, int depth, int& remaining) {
  if (depth > 64 || --remaining < 0) throw std::invalid_argument("Registered custom type exceeds its AST bound");
  FunctionOpaqueRegisteredType result;
  result.set_name(source.baseName());
  result.set_variable_arity(source.variableArity());
  if (source.rowFieldName()) result.set_row_field_name(*source.rowFieldName());
  for (const auto& parameter : source.parameters()) *result.add_parameters() = opaque(parameter, depth + 1, remaining);
  return result;
}

FunctionTypeFormula parameter(const TypeSignature& source, const Signature& signature) {
  if (!source.parameters().empty() || source.variableArity()) throw std::invalid_argument("Nested registered integer parameter");
  FunctionTypeFormula result;
  const auto& name = source.baseName();
  const auto found = signature.variables().find(name);
  if (found != signature.variables().end()) {
    if (!found->second.isIntegerParameter()) throw std::invalid_argument("Registered Native integer parameter has a different kind");
    result.set_operation(FUNCTION_FORMULA_OPERATION_VARIABLE);
    result.set_variable_name(name);
    return result;
  }
  int32_t value = 0;
  const auto [end, error] = std::from_chars(name.data(), name.data() + name.size(), value);
  if (name.empty() || !std::all_of(name.begin(), name.end(), [](char character) { return character >= '0' && character <= '9'; })
      || error != std::errc{} || end != name.data() + name.size()) {
    throw std::invalid_argument("Registered Native integer literal is not exact signed int32");
  }
  result.set_operation(FUNCTION_FORMULA_OPERATION_INTEGER);
  result.set_integer_value(value);
  return result;
}

Message* childParent(Message* root, const FunctionTypeProjectionChildPath& declaration,
                     const Field*& field) {
  const auto& path = declaration.field_numbers();
  if (path.empty() || path.size() > 64) {
    throw std::invalid_argument("Registered canonical child collection path is invalid");
  }
  Message* current = root;
  for (int position = 0; position < path.size(); ++position) {
    field = current->GetDescriptor()->FindFieldByNumber(path.Get(position));
    if (!field || field->cpp_type() != Field::CPPTYPE_MESSAGE) {
      throw std::invalid_argument("Registered canonical child collection path traverses a scalar");
    }
    // A declaration names the terminal child collection, not one indexed
    // substitution slot. Each real child ordinal is attached below, after
    // the container is materialized; intermediate collections remain invalid.
    if (position == path.size() - 1) return current;
    if (field->is_repeated()) {
      throw std::invalid_argument("Registered canonical child path has an unindexed intermediate collection");
    }
    current = current->GetReflection()->MutableMessage(current, field);
  }
  throw std::invalid_argument("Registered type path is empty");
}

const FunctionPhysicalTypeProjection* projection(
    const FunctionCatalogManifest& declarations, const TypeSignature& source) {
  const auto name = upper(source.baseName());
  const FunctionPhysicalTypeProjection* result = nullptr;
  for (const auto& candidate : declarations.physical_type_projections()) {
    if (candidate.native_type_name() != name) continue;
    if (result && result->binding_type().SerializeAsString() != candidate.binding_type().SerializeAsString()) {
      throw std::invalid_argument("Native signature has ambiguous canonical type projections");
    }
    result = &candidate;
  }
  return result;
}

class Converter final {
 public:
  Converter(const FunctionCatalogManifest& projections, const Signature& signature)
      : projections_(projections), signature_(signature) {}

  FunctionTypeTemplate convert(const TypeSignature& source, int depth = 0) {
    if (depth > 64 || --remaining_ < 0) throw std::invalid_argument("Registered canonical type AST exceeds its bound");
    FunctionTypeTemplate result;
    if (source.baseName() == "any") { result.mutable_any_type(); return result; }
    const auto variable = signature_.variables().find(source.baseName());
    if (variable != signature_.variables().end()) {
      if (!variable->second.isTypeParameter() || !source.parameters().empty() || source.variableArity()) {
        throw std::invalid_argument("Registered type variable has malformed nested parameters");
      }
      result.set_type_variable(source.baseName());
      return result;
    }
    const auto* declaration = projection(projections_, source);
    if (!declaration) {
      *result.mutable_opaque_registered_type() = opaque(source, depth, remaining_);
      return result;
    }
    result = declaration->binding_type();
    appendIntegers(result, source, *declaration);
    appendChildren(result, source, *declaration, depth);
    result.set_homogeneous_row(source.isHomogeneousRow());
    return result;
  }

 private:
  void appendIntegers(FunctionTypeTemplate& target, const TypeSignature& source,
                      const FunctionPhysicalTypeProjection& declaration) {
    const size_t count = declaration.native_integer_parameter_paths_size();
    if (source.parameters().size() < count) throw std::invalid_argument("Registered signature omits Native integer parameters");
    for (size_t ordinal = 0; ordinal < count; ++ordinal) {
      const auto& path = declaration.native_integer_parameter_paths(static_cast<int>(ordinal)).path();
      FunctionTypeSlot* matched = nullptr;
      for (auto& slot : *target.mutable_slots()) {
        if (slot.path().size() != path.size()) continue;
        bool equal = true;
        for (int index = 0; index < path.size(); ++index) equal &= slot.path(index).SerializeAsString() == path.Get(index).SerializeAsString();
        if (equal) { matched = &slot; break; }
      }
      if (!matched) throw std::invalid_argument("Generated Native integer path has no canonical substitution");
      *matched->mutable_integer() = parameter(source.parameters()[ordinal], signature_);
    }
  }

  void appendChildren(FunctionTypeTemplate& target, const TypeSignature& source,
                      const FunctionPhysicalTypeProjection& declaration, int depth) {
    size_t parameterOrdinal = declaration.native_integer_parameter_paths_size();
    for (const auto& child : declaration.recursive_children()) {
      FunctionTypeSlot address;
      for (auto field : child.field_numbers()) address.add_path()->set_field_number(field);
      const Field* field = nullptr;
      auto* parent = childParent(target.mutable_skeleton(), child, field);
      const size_t count = field->is_repeated() ? source.parameters().size() - parameterOrdinal : 1;
      if (field->is_repeated()) parent->GetReflection()->ClearField(parent, field);
      for (size_t index = 0; index < count; ++index) {
        if (parameterOrdinal >= source.parameters().size()) throw std::invalid_argument("Registered signature omits a Native child type");
        const auto& parameter = source.parameters()[parameterOrdinal++];
        auto path = address.path();
        if (field->is_repeated()) path.Mutable(path.size() - 1)->set_repeated_index(static_cast<uint32_t>(index));
        auto* placeholder = field->is_repeated() ? parent->GetReflection()->AddMessage(parent, field)
                                                : parent->GetReflection()->MutableMessage(parent, field);
        substrait::Type value;
        value.mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
        placeholder->CopyFrom(value);
        auto* slot = target.add_slots();
        *slot->mutable_path() = path;
        *slot->mutable_type() = convert(parameter, depth + 1);
        if (parameter.rowFieldName() && !source.isHomogeneousRow()) {
          auto* constraint = target.add_field_name_constraints();
          *constraint->mutable_path() = path;
          constraint->set_name(*parameter.rowFieldName());
        }
      }
    }
    if (parameterOrdinal != source.parameters().size()) throw std::invalid_argument("Registered signature has unused Native type parameters");
  }

  const FunctionCatalogManifest& projections_;
  const Signature& signature_;
  int remaining_{4096};
};
}  // namespace

RegisteredTypeTemplate::RegisteredTypeTemplate() { generated::appendCanonicalPhysicalProjections(&projections_); }

protocol::v2::FunctionTypeTemplate RegisteredTypeTemplate::convert(
    const facebook::velox::exec::TypeSignature& type,
    const facebook::velox::exec::FunctionSignature& signature) const {
  return Converter(projections_, signature).convert(type);
}
}  // namespace pangrova::capability
