// Canonical Substrait type and expression structure conversion.
// Literals and scalar binding live in their own conversion units.
#include "velox/plan/VeloxPlanConverter.h"
#include "capability/CanonicalPhysicalTypes.h"

#include "velox/core/Expressions.h"
#include "velox/type/Type.h"

namespace pangrova::velox {

facebook::velox::TypePtr VeloxPlanConverter::convertType(
    const substrait::Type& type) {
  return pangrova::capability::canonicalPhysicalType(type);
}

facebook::velox::RowTypePtr VeloxPlanConverter::convertNamedStruct(
    const substrait::NamedStruct& named_struct) {
  return pangrova::capability::canonicalPhysicalSchema(named_struct);
}

// ---------------------------------------------------------------------------
// Expression conversion
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertFieldReference(
    const substrait::Expression_FieldReference& reference,
    const facebook::velox::RowTypePtr& inputType) {
  facebook::velox::core::TypedExprPtr root;
  if (reference.has_root_reference()) {
    root = std::make_shared<facebook::velox::core::InputTypedExpr>(inputType);
    if (reference.has_direct_reference() &&
        !reference.direct_reference().has_struct_field()) {
      throw std::runtime_error("Root field reference requires a struct_field");
    }
  } else if (reference.has_expression()) {
    root = convertExpression(reference.expression(), inputType);
  } else {
    throw std::runtime_error("Field reference requires an explicit supported root");
  }
  if (reference.has_direct_reference()) {
    return convertReferenceSegment(reference.direct_reference(), root);
  }
  if (reference.has_masked_reference()) {
    const auto& mask = reference.masked_reference();
    return convertStructMask(mask.select(), root, mask.maintain_singular_struct());
  }
  throw std::runtime_error("Malformed field reference: no declared selection");
}

facebook::velox::core::TypedExprPtr VeloxPlanConverter::convertReferenceSegment(
    const substrait::Expression::ReferenceSegment& segment,
    facebook::velox::core::TypedExprPtr value) {
  using namespace facebook::velox;
  core::TypedExprPtr selected;
  const substrait::Expression::ReferenceSegment* child = nullptr;
  if (segment.has_struct_field()) {
    const auto& field = segment.struct_field();
    const auto row = std::dynamic_pointer_cast<const RowType>(value->type());
    const auto index = field.field();
    if (!row || index < 0 || static_cast<size_t>(index) >= row->size()) {
      throw std::runtime_error("Field reference index is out of bounds for ROW schema");
    }
    selected = value->isInputKind()
        ? core::TypedExprPtr(std::make_shared<core::FieldAccessTypedExpr>(
              row->childAt(index), row->nameOf(index)))
        : core::TypedExprPtr(std::make_shared<core::DereferenceTypedExpr>(
              row->childAt(index), value, index));
    if (field.has_child()) child = &field.child();
  } else if (segment.has_list_element()) {
    const auto array = std::dynamic_pointer_cast<const ArrayType>(value->type());
    if (!array) throw std::runtime_error("List reference requires an ARRAY value");
    const auto& element = segment.list_element();
    const int64_t offset = element.offset();
    auto index = std::make_shared<core::ConstantTypedExpr>(
        BIGINT(), variant(offset < 0 ? offset : offset + 1));
    selected = checkedScalarCall("element_at", array->elementType(), {value, index});
    if (element.has_child()) child = &element.child();
  } else if (segment.has_map_key()) {
    const auto map = std::dynamic_pointer_cast<const MapType>(value->type());
    if (!map) throw std::runtime_error("Map reference requires a MAP value");
    const auto& key = segment.map_key();
    if (!key.has_map_key()) throw std::runtime_error("Map reference requires a typed key literal");
    auto literal = convertLiteral(key.map_key());
    if (!literal->type()->equivalent(*map->keyType())) {
      throw std::runtime_error("Map reference key type does not match declared MAP key");
    }
    selected = checkedScalarCall("element_at", map->valueType(), {value, literal});
    if (key.has_child()) child = &key.child();
  } else {
    throw std::runtime_error("Malformed field reference segment");
  }
  return child ? convertReferenceSegment(*child, selected) : selected;
}

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertExpression(
    const substrait::Expression& expr,
    const facebook::velox::RowTypePtr& input_type) {
  switch (expr.rex_type_case()) {
    case substrait::Expression::kSelection:
      return convertFieldReference(expr.selection(), input_type);
    case substrait::Expression::kLiteral:
      return convertLiteral(expr.literal());
    case substrait::Expression::kScalarFunction:
      return convertScalarFunction(expr.scalar_function(), input_type);
    case substrait::Expression::kCast: {
      const auto& cast = expr.cast();
      auto input = convertExpression(cast.input(), input_type);
      return std::make_shared<facebook::velox::core::CastTypedExpr>(
          convertType(cast.type()),
          std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>{
              std::move(input)},
          /*nullOnFailure=*/false);
    }
    case substrait::Expression::kIfThen:
      return convertIfThen(expr.if_then(), input_type);
    case substrait::Expression::kNested:
      return convertNestedExpression(expr.nested(), input_type);
    case substrait::Expression::kWindowFunction:
      return convertWindowFunction(expr.window_function(), input_type);
    default:
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported Substrait expression type: " +
          std::to_string(static_cast<int>(expr.rex_type_case())));
  }
}

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertIfThen(
    const substrait::Expression_IfThen& expression,
    const facebook::velox::RowTypePtr& input_type) {
  if (expression.ifs_size() == 0) {
    if (expression.has_else_()) {
      return convertExpression(expression.else_(), input_type);
    }
    throw std::runtime_error(
        "VeloxPlanConverter: IfThen expression has no clauses and no else");
  }
  auto result_type =
      convertExpression(expression.ifs(0).then(), input_type)->type();
  auto current = expression.has_else_()
      ? convertExpression(expression.else_(), input_type)
      : std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            result_type, facebook::velox::variant::null(result_type->kind()));
  for (int i = expression.ifs_size() - 1; i >= 0; --i) {
    auto condition = convertExpression(expression.ifs(i).if_(), input_type);
    auto then_branch = convertExpression(expression.ifs(i).then(), input_type);
    current = checkedScalarCall(
        "if", result_type, {condition, then_branch, current});
  }
  return current;
}

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertWindowFunction(
    const substrait::Expression_WindowFunction& function,
    const facebook::velox::RowTypePtr& input_type) {
  auto anchor = function_map_.find(function.function_reference());
  if (anchor == function_map_.end()) {
    throw std::runtime_error("VeloxPlanConverter: unbound window function anchor");
  }
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> args;
  for (const auto& argument : function.arguments()) {
    if (!argument.has_value()) {
      throw std::runtime_error(
          "VeloxPlanConverter: window arguments require value expressions");
    }
    args.push_back(convertExpression(argument.value(), input_type));
  }
  return std::make_shared<facebook::velox::core::CallTypedExpr>(
      convertType(function.output_type()), std::move(args), anchor->second);
}

}  // namespace pangrova::velox
