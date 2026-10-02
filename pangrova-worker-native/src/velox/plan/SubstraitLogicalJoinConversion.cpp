#include "velox/plan/VeloxPlanConverter.h"

#include "velox/plan/CanonicalJoinSemantics.h"

namespace pangrova::velox {
using namespace facebook::velox;

void VeloxPlanConverter::extractJoinKeys(
    const substrait::Expression& expression, const RowTypePtr& leftType,
    const RowTypePtr& rightType, const RowTypePtr& /*combinedType*/,
    int leftCount, std::vector<core::FieldAccessTypedExprPtr>& leftKeys,
    std::vector<core::FieldAccessTypedExprPtr>& rightKeys) {
  if (!expression.has_scalar_function()) return;
  const auto& function = expression.scalar_function();
  const auto found = function_map_.find(function.function_reference());
  if (found == function_map_.end()) throw std::runtime_error("Join has an unbound function");
  if (found->second == "and") {
    for (const auto& argument : function.arguments()) {
      if (!argument.has_value()) throw std::runtime_error("Join AND requires value arguments");
      extractJoinKeys(argument.value(), leftType, rightType, nullptr,
          leftCount, leftKeys, rightKeys);
    }
    return;
  }
  if ((found->second != "eq" && found->second != "equal") ||
      function.arguments_size() != 2) return;
  auto ordinal = [](const substrait::FunctionArgument& argument) {
    if (!argument.has_value() || !argument.value().has_selection()) return -1;
    const auto& reference = argument.value().selection();
    if (!reference.has_root_reference() || !reference.has_direct_reference() ||
        !reference.direct_reference().has_struct_field()) return -1;
    const auto& field = reference.direct_reference().struct_field();
    return field.has_child() ? -1 : field.field();
  };
  auto l = ordinal(function.arguments(0));
  auto r = ordinal(function.arguments(1));
  if (l >= leftCount && r >= 0 && r < leftCount) std::swap(l, r);
  if (l < 0 || l >= leftCount || r < leftCount ||
      static_cast<size_t>(r - leftCount) >= rightType->size()) return;
  if (!leftType->childAt(l)->equivalent(*rightType->childAt(r - leftCount))) {
    throw std::runtime_error("Join key declared types do not match");
  }
  leftKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
      leftType->childAt(l), leftType->nameOf(l)));
  rightKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
      rightType->childAt(r - leftCount), rightType->nameOf(r - leftCount)));
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertJoinRel(
    const substrait::JoinRel& relation, const std::vector<split::SplitConfig>& splits) {
  auto left = convertRel(relation.left(), splits);
  auto right = rebaseJoinInput(convertRel(relation.right(), splits), left->outputType()->size());
  const auto kind = canonicalJoinKind(relation.type());
  const auto combined = combinedJoinType(left->outputType(), right->outputType());
  auto condition = relation.has_expression() ? convertExpression(relation.expression(), combined)
      : core::TypedExprPtr{};
  if (condition && !condition->type()->isBoolean()) {
    throw std::runtime_error("Join condition must have BOOLEAN type");
  }
  PreparedJoinKeys keys;
  keys.left = left;
  keys.right = right;
  keys.condition = condition;
  if (relation.has_expression()) {
    extractJoinKeys(relation.expression(), left->outputType(), right->outputType(),
        combined, left->outputType()->size(), keys.leftKeys, keys.rightKeys);
  }
  auto joined = keys.leftKeys.empty() || kind.ends_with("_MARK")
      ? buildNestedJoin(kind, left, right, condition)
      : buildKeyedJoin(kind, std::move(keys),
          canonicalJoinOutput(kind, left->outputType(), right->outputType()), false, false);
  if (relation.has_post_join_filter()) joined = applyPostJoinFilter(relation.post_join_filter(), joined);
  return joined;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertNestedLoopJoinRel(
    const substrait::NestedLoopJoinRel& relation,
    const std::vector<split::SplitConfig>& splits) {
  auto left = convertRel(relation.left(), splits);
  auto right = rebaseJoinInput(convertRel(relation.right(), splits), left->outputType()->size());
  auto condition = relation.has_expression()
      ? convertExpression(relation.expression(), combinedJoinType(left->outputType(), right->outputType()))
      : core::TypedExprPtr{};
  return buildNestedJoin(canonicalJoinKind(relation.type()), left, right, condition);
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertCrossRel(
    const substrait::CrossRel& relation, const std::vector<split::SplitConfig>& splits) {
  auto left = convertRel(relation.left(), splits);
  auto right = rebaseJoinInput(convertRel(relation.right(), splits), left->outputType()->size());
  return buildNestedJoin("JOIN_TYPE_INNER", left, right, nullptr);
}
}  // namespace pangrova::velox
