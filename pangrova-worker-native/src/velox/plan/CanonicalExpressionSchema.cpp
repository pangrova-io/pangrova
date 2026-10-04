#include "velox/plan/CanonicalPlanSchema.h"

#include <stdexcept>

namespace pangrova::velox::schema {
namespace {
using Type = substrait::Type;

Type declared(Type type) {
  validateValueType(type);
  return type;
}

Type literal(const substrait::Expression_Literal& value);

Type scalarLiteral(const substrait::Expression_Literal& value) {
  const auto field = value.GetDescriptor()->FindFieldByNumber(value.literal_type_case());
  if (!field) throw std::invalid_argument("Literal requires a declared kind");
  std::string kind = field->name();
  if (kind == "boolean") kind = "bool";
  else if (kind == "var_char") kind = "varchar";
  else if (kind == "interval_year_to_month") kind = "interval_year";
  else if (kind == "interval_day_to_second") kind = "interval_day";
  auto result = primitive(kind, Type::NULLABILITY_REQUIRED);
  const auto targetField = result.GetDescriptor()->FindFieldByName(kind);
  auto target = result.GetReflection()->MutableMessage(&result, targetField);
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
    const auto& source = value.GetReflection()->GetMessage(value, field);
    for (const auto name : {"precision", "scale", "length", "type_reference"}) {
      const auto src = source.GetDescriptor()->FindFieldByName(name);
      const auto dst = target->GetDescriptor()->FindFieldByName(name);
      if (src && dst) {
        if (src->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT32) {
          target->GetReflection()->SetInt32(target, dst, source.GetReflection()->GetInt32(source, src));
        } else if (src->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT32) {
          target->GetReflection()->SetUInt32(target, dst, source.GetReflection()->GetUInt32(source, src));
        }
      }
    }
  } else if (value.has_fixed_binary()) result.mutable_fixed_binary()->set_length(value.fixed_binary().size());
  else if (value.has_fixed_char()) {
    int codepoints = 0;
    for (const unsigned char byte : value.fixed_char()) if ((byte & 0xc0) != 0x80) ++codepoints;
    result.mutable_fixed_char()->set_length(codepoints);
  }
  return result;
}

Type literal(const substrait::Expression_Literal& value) {
  Type result;
  if (value.has_null()) return declared(value.null());
  if (value.has_empty_list()) { *result.mutable_list() = value.empty_list(); return declared(result); }
  if (value.has_empty_map()) { *result.mutable_map() = value.empty_map(); return declared(result); }
  if (value.has_struct_()) {
    for (const auto& child : value.struct_().fields()) *result.mutable_struct_()->add_types() = literal(child);
  } else if (value.has_list()) {
    std::vector<Type> values;
    for (const auto& child : value.list().values()) values.push_back(literal(child));
    *result.mutable_list()->mutable_type() = merge(values);
  } else if (value.has_map()) {
    std::vector<Type> keys, values;
    for (const auto& pair : value.map().key_values()) { keys.push_back(literal(pair.key())); values.push_back(literal(pair.value())); }
    *result.mutable_map()->mutable_key() = merge(keys);
    *result.mutable_map()->mutable_value() = merge(values);
  } else result = scalarLiteral(value);
  result = withNullability(std::move(result), value.nullable() ? Type::NULLABILITY_NULLABLE : Type::NULLABILITY_REQUIRED);
  const auto field = result.GetDescriptor()->FindFieldByNumber(result.kind_case());
  auto target = result.GetReflection()->MutableMessage(&result, field);
  const auto variation = target->GetDescriptor()->FindFieldByName("type_variation_reference");
  if (variation) target->GetReflection()->SetUInt32(target, variation, value.type_variation_reference());
  return declared(result);
}

Type segment(const substrait::Expression_ReferenceSegment& path, const Type& root) {
  Type selected;
  const substrait::Expression_ReferenceSegment* child = nullptr;
  if (path.has_struct_field()) {
    const auto& field = path.struct_field();
    if (!root.has_struct_() || field.field() < 0 || field.field() >= root.struct_().types_size()) {
      throw std::invalid_argument("Canonical struct path is out of bounds");
    }
    selected = root.struct_().types(field.field());
    if (nullable(root)) selected = withNullability(std::move(selected), Type::NULLABILITY_NULLABLE);
    if (field.has_child()) child = &field.child();
  } else if (path.has_list_element()) {
    if (!root.has_list()) throw std::invalid_argument("Canonical list path requires a list");
    selected = withNullability(root.list().type(), Type::NULLABILITY_NULLABLE);
    if (path.list_element().has_child()) child = &path.list_element().child();
  } else if (path.has_map_key()) {
    if (!root.has_map()) throw std::invalid_argument("Canonical map path requires a map");
    selected = withNullability(root.map().value(), Type::NULLABILITY_NULLABLE);
    if (path.map_key().has_child()) child = &path.map_key().child();
  } else throw std::invalid_argument("Canonical field path has no supported segment");
  return child ? segment(*child, selected) : selected;
}

Type reference(const substrait::Expression_FieldReference& value, const substrait::NamedStruct& input) {
  Type root;
  if (value.has_root_reference()) {
    fields(input);
    *root.mutable_struct_() = input.struct_();
    // NamedStruct's outer container is not a nullable row value.
    if (root.struct_().nullability() == Type::NULLABILITY_UNSPECIFIED) {
      root.mutable_struct_()->set_nullability(Type::NULLABILITY_REQUIRED);
    }
  }
  else if (value.has_expression()) root = expression(value.expression(), input);
  else throw std::invalid_argument("Canonical field path requires an explicit supported root");
  if (value.has_masked_reference()) {
    if (!root.has_struct_()) throw std::invalid_argument("Canonical mask requires a struct root");
    auto field = computed("root", root);
    substrait::NamedStruct schema;
    *schema.mutable_struct_() = root.struct_();
    for (const auto& name : field.descendants) schema.add_names(name);
    const auto selected = mask(schema, value.masked_reference());
    if (!value.masked_reference().maintain_singular_struct() && selected.struct_().types_size() == 1) {
      return selected.struct_().types(0);
    }
    *root.mutable_struct_() = selected.struct_();
    return root;
  }
  if (!value.has_direct_reference()) throw std::invalid_argument("Canonical field path has no selection");
  return segment(value.direct_reference(), root);
}

Type nested(const substrait::Expression_Nested& value, const substrait::NamedStruct& input) {
  Type result;
  if (value.has_struct_()) {
    for (const auto& child : value.struct_().fields()) *result.mutable_struct_()->add_types() = expression(child, input);
  } else if (value.has_list()) {
    std::vector<Type> elements;
    for (const auto& child : value.list().values()) elements.push_back(expression(child, input));
    *result.mutable_list()->mutable_type() = merge(elements);
  } else throw std::invalid_argument("Nested expression requires its declared supported constructor");
  result = withNullability(std::move(result), value.nullable() ? Type::NULLABILITY_NULLABLE : Type::NULLABILITY_REQUIRED);
  const auto field = result.GetDescriptor()->FindFieldByNumber(result.kind_case());
  auto target = result.GetReflection()->MutableMessage(&result, field);
  const auto variation = target->GetDescriptor()->FindFieldByName("type_variation_reference");
  if (variation) target->GetReflection()->SetUInt32(target, variation, value.type_variation_reference());
  return result;
}
}  // namespace

Type expression(const substrait::Expression& value, const substrait::NamedStruct& input) {
  switch (value.rex_type_case()) {
    case substrait::Expression::kLiteral: return literal(value.literal());
    case substrait::Expression::kSelection: return reference(value.selection(), input);
    case substrait::Expression::kScalarFunction: return declared(value.scalar_function().output_type());
    case substrait::Expression::kWindowFunction: return declared(value.window_function().output_type());
    case substrait::Expression::kCast: return declared(value.cast().type());
    case substrait::Expression::kNested: return nested(value.nested(), input);
    case substrait::Expression::kSingularOrList:
    case substrait::Expression::kMultiOrList: return primitive("bool", Type::NULLABILITY_NULLABLE);
    case substrait::Expression::kIfThen: {
      std::vector<Type> branches;
      for (const auto& clause : value.if_then().ifs()) {
        if (!expression(clause.if_(), input).has_bool_()) throw std::invalid_argument("IF condition must be BOOLEAN");
        branches.push_back(expression(clause.then(), input));
      }
      branches.push_back(expression(value.if_then().else_(), input));
      return merge(branches);
    }
    default: throw std::invalid_argument("Expression has no supported canonical output schema");
  }
}

Field expressionField(const substrait::Expression& value, const substrait::NamedStruct& input, std::string name) {
  if (value.has_selection()) {
    const auto& selected = value.selection();
    if (selected.has_root_reference() && selected.has_direct_reference() && selected.direct_reference().has_struct_field()) {
      const auto& field = selected.direct_reference().struct_field();
      const auto source = fields(input);
      if (!field.has_child() && field.field() >= 0 && static_cast<size_t>(field.field()) < source.size()) return source[field.field()];
    }
  }
  return computed(std::move(name), expression(value, input));
}
}  // namespace pangrova::velox::schema
