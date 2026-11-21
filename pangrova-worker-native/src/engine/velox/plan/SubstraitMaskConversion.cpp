// Canonical masks lower to registered Velox expressions, not another executor.
#include "engine/velox/plan/VeloxPlanConverter.h"

#include <stdexcept>

#include "engine/velox/plan/CanonicalOrdinalIdentity.h"

namespace pangrova::velox {
using namespace facebook::velox;

namespace {
core::TypedExprPtr integer(int64_t value) {
  return std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(value));
}

core::TypedExprPtr field(const core::TypedExprPtr& row, int32_t index) {
  const auto type = std::dynamic_pointer_cast<const RowType>(row->type());
  if (!type || index < 0 || static_cast<size_t>(index) >= type->size()) {
    throw std::runtime_error("Mask struct field is out of bounds");
  }
  if (row->isInputKind()) {
    return std::make_shared<core::FieldAccessTypedExpr>(
        type->childAt(index), type->nameOf(index));
  }
  return std::make_shared<core::DereferenceTypedExpr>(
      type->childAt(index), row, index);
}
}  // namespace

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertReadProjection(
    const substrait::Expression::MaskExpression& mask,
    std::shared_ptr<const core::PlanNode> input) {
  const auto type = input->outputType();
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> expressions;
  for (const auto& item : mask.select().struct_items()) {
    const auto index = item.field();
    if (index < 0 || static_cast<size_t>(index) >= type->size()) {
      throw std::runtime_error("ReadRel mask field is out of bounds");
    }
    core::TypedExprPtr expression = std::make_shared<core::FieldAccessTypedExpr>(
        type->childAt(index), type->nameOf(index));
    if (item.has_child()) {
      expression = convertMaskSelection(
          item.child(), expression, mask.maintain_singular_struct());
    }
    names.push_back(physicalFieldAlias(names.size()));
    expressions.push_back(std::move(expression));
  }
  // A relation is always a record, including one/zero columns. Only nested
  // selections unwrap when maintain_singular_struct is false.
  return std::make_shared<core::ProjectNode>(
      nextNodeId(), std::move(names), std::move(expressions), input);
}

core::TypedExprPtr VeloxPlanConverter::convertMaskSelection(
    const substrait::Expression::MaskExpression::Select& selection,
    core::TypedExprPtr value, bool maintain) {
  if (selection.has_struct_()) {
    return convertStructMask(selection.struct_(), std::move(value), maintain);
  }
  if (selection.has_list()) {
    return convertListMask(selection.list(), std::move(value), maintain);
  }
  if (selection.has_map()) {
    return convertMapMask(selection.map(), std::move(value), maintain);
  }
  throw std::runtime_error("Mask child has no declared selection");
}

core::TypedExprPtr VeloxPlanConverter::convertStructMask(
    const substrait::Expression::MaskExpression::StructSelect& selection,
    core::TypedExprPtr value, bool maintain) {
  const auto rowType = std::dynamic_pointer_cast<const RowType>(value->type());
  if (!rowType) {
    throw std::runtime_error("Struct mask requires a ROW value");
  }
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<core::TypedExprPtr> expressions;
  for (const auto& item : selection.struct_items()) {
    auto child = field(value, item.field());
    if (item.has_child()) {
      child = convertMaskSelection(item.child(), child, maintain);
    }
    names.push_back(rowType->nameOf(item.field()));
    types.push_back(child->type());
    expressions.push_back(std::move(child));
  }
  if (!maintain && expressions.size() == 1) {
    return expressions.front();
  }
  auto type = ROW(std::move(names), std::move(types));
  auto projected = checkedScalarCall("row_constructor", type, std::move(expressions));
  if (value->isInputKind()) {
    return projected;
  }
  // Constructing a ROW from dereferenced null fields would turn a NULL parent
  // into a non-null ROW of NULL children. Preserve the original parent NULL.
  return checkedScalarCall("if", type,
      {checkedScalarCall("is_null", BOOLEAN(), {value}),
       core::ConstantTypedExpr::makeNull(type), projected});
}

core::TypedExprPtr VeloxPlanConverter::convertListMask(
    const substrait::Expression::MaskExpression::ListSelect& selection,
    core::TypedExprPtr value, bool maintain) {
  const auto type = std::dynamic_pointer_cast<const ArrayType>(value->type());
  if (!type) {
    throw std::runtime_error("List mask requires an ARRAY value");
  }
  if (!maintain && selection.selection_size() == 1 &&
      selection.selection(0).has_item()) {
    int64_t index = selection.selection(0).item().field();
    auto element = checkedScalarCall("element_at", type->elementType(),
        {value, integer(index < 0 ? index : index + 1)});
    return selection.has_child()
        ? convertMaskSelection(selection.child(), element, maintain) : element;
  }
  core::TypedExprPtr projected = value;
  if (selection.selection_size() > 0) {
    std::vector<core::TypedExprPtr> fragments;
    const auto length = checkedScalarCall("cardinality", BIGINT(), {value});
    auto endpoint = [&](int64_t index) {
      auto position = index < 0
          ? checkedScalarCall("plus", BIGINT(), {length, integer(index)})
          : integer(index);
      return checkedScalarCall("greatest", BIGINT(),
          {integer(0), checkedScalarCall("least", BIGINT(), {length, position})});
    };
    for (const auto& item : selection.selection()) {
      int64_t start;
      int64_t end;
      if (item.has_item()) {
        start = item.item().field();
        end = start == -1 ? 0 : start + 1;
      } else if (item.has_slice()) {
        start = item.slice().start();
        end = item.slice().end();
      } else {
        throw std::runtime_error("List mask item has no selection");
      }
      auto begin = endpoint(start);
      auto finish = item.has_item() && start == -1 ? length : endpoint(end);
      fragments.push_back(checkedScalarCall("slice", type,
          {value, checkedScalarCall("plus", BIGINT(), {begin, integer(1)}),
           checkedScalarCall("greatest", BIGINT(),
               {integer(0), checkedScalarCall("minus", BIGINT(), {finish, begin})})}));
    }
    projected = fragments.size() == 1 ? fragments.front()
        : checkedScalarCall("concat", type, std::move(fragments));
  }
  if (!selection.has_child()) {
    return projected;
  }
  const auto itemName = "$pangrova$mask_item_" + nextNodeId();
  auto item = std::make_shared<core::FieldAccessTypedExpr>(type->elementType(), itemName);
  auto body = convertMaskSelection(selection.child(), item, maintain);
  auto lambda = std::make_shared<core::LambdaTypedExpr>(
      ROW({itemName}, {type->elementType()}), body);
  return checkedScalarCall("transform", ARRAY(body->type()), {projected, lambda});
}

core::TypedExprPtr VeloxPlanConverter::convertMapMask(
    const substrait::Expression::MaskExpression::MapSelect& selection,
    core::TypedExprPtr value, bool maintain) {
  const auto type = std::dynamic_pointer_cast<const MapType>(value->type());
  if (!type || !type->keyType()->isVarchar()) {
    throw std::runtime_error("Map mask string keys require a MAP(VARCHAR, value)");
  }
  if (selection.has_expression()) {
    // The specification explicitly leaves the simplified-regex format
    // undefined. Never silently substitute another regex language.
    throw std::runtime_error("Map mask wildcard grammar is not defined by Substrait");
  }
  const auto keyName = "$pangrova$mask_key_" + nextNodeId();
  const auto valueName = "$pangrova$mask_value_" + nextNodeId();
  auto key = std::make_shared<core::FieldAccessTypedExpr>(type->keyType(), keyName);
  auto item = std::make_shared<core::FieldAccessTypedExpr>(type->valueType(), valueName);
  const auto signature = ROW({keyName, valueName}, {type->keyType(), type->valueType()});
  core::TypedExprPtr projected = value;
  if (selection.has_key()) {
    auto selectedKey = std::make_shared<core::ConstantTypedExpr>(
        VARCHAR(), variant(selection.key().map_key()));
    auto predicate = checkedScalarCall("eq", BOOLEAN(), {key, selectedKey});
    auto lambda = std::make_shared<core::LambdaTypedExpr>(signature, predicate);
    projected = checkedScalarCall("map_filter", type, {value, lambda});
  }
  if (!selection.has_child()) {
    return projected;
  }
  auto body = convertMaskSelection(selection.child(), item, maintain);
  auto lambda = std::make_shared<core::LambdaTypedExpr>(signature, body);
  return checkedScalarCall("transform_values", MAP(type->keyType(), body->type()),
                           {projected, lambda});
}

}  // namespace pangrova::velox
