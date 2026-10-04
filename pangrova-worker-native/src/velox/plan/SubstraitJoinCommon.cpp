// One set of Velox join semantics, independent of relation enum numbering.
#include "velox/plan/VeloxPlanConverter.h"

#include "velox/plan/CanonicalJoinSemantics.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace pangrova::velox {
using namespace facebook::velox;

namespace {
bool hasExactMergeOrder(core::PlanNodePtr input,
                       std::vector<core::FieldAccessTypedExprPtr> keys) {
  while (const auto project = std::dynamic_pointer_cast<const core::ProjectNode>(input)) {
    if (project->sources().size() != 1) return false;
    const auto source = project->sources().front();
    for (auto& key : keys) {
      const auto ordinal = project->outputType()->getChildIdxIfExists(key->name());
      if (!ordinal) return false;
      const auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
          project->projections().at(*ordinal));
      if (!field || !field->inputs().empty() || !field->type()->equivalent(*key->type())) return false;
      const auto sourceOrdinal = source->outputType()->getChildIdxIfExists(field->name());
      if (!sourceOrdinal || !field->type()->equivalent(*source->outputType()->childAt(*sourceOrdinal))) return false;
      key = field;
    }
    input = source;
  }
  const auto sort = std::dynamic_pointer_cast<const core::OrderByNode>(input);
  if (!sort || sort->isPartial() || sort->sortingKeys().size() < keys.size()) return false;
  for (size_t i = 0; i < keys.size(); ++i) {
    const auto& field = sort->sortingKeys().at(i);
    const auto& order = sort->sortingOrders().at(i);
    if (!field->inputs().empty() || field->name() != keys[i]->name() ||
        !field->type()->equivalent(*keys[i]->type()) ||
        !order.isAscending() || !order.isNullsFirst()) return false;
  }
  return true;
}
}  // namespace

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::rebaseJoinInput(
    std::shared_ptr<const core::PlanNode> input, size_t offset) {
  const auto& type = input->outputType();
  const auto physical = physicalRowType(type, offset);
  if (type->names() == physical->names()) return input;
  std::vector<core::TypedExprPtr> expressions;
  for (size_t i = 0; i < type->size(); ++i) {
    expressions.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        type->childAt(i), type->nameOf(i)));
  }
  return std::make_shared<core::ProjectNode>(
      nextNodeId(), physical->names(), expressions, input);
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::applyPostJoinFilter(
    const substrait::Expression& predicate,
    std::shared_ptr<const core::PlanNode> input) {
  auto expression = convertExpression(predicate, input->outputType());
  if (!expression->type()->isBoolean()) {
    throw std::runtime_error("Join post_join_filter must have BOOLEAN type");
  }
  return std::make_shared<core::FilterNode>(nextNodeId(), expression, input);
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::finishJoinMarker(
    std::shared_ptr<const core::PlanNode> input,
    const RowTypePtr& outputType, bool anti) {
  const auto& type = input->outputType();
  auto marker = std::make_shared<core::FieldAccessTypedExpr>(
      BOOLEAN(), type->nameOf(type->size() - 1));
  core::TypedExprPtr predicate = marker;
  if (anti) predicate = checkedScalarCall("not", BOOLEAN(), {marker});
  auto filtered = std::make_shared<core::FilterNode>(nextNodeId(), predicate, input);
  std::vector<core::TypedExprPtr> projections;
  for (size_t i = 0; i < outputType->size(); ++i) {
    projections.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        outputType->childAt(i), outputType->nameOf(i)));
  }
  return std::make_shared<core::ProjectNode>(
      nextNodeId(), outputType->names(), projections, filtered);
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::buildNestedJoin(
    const std::string& kind, std::shared_ptr<const core::PlanNode> left,
    std::shared_ptr<const core::PlanNode> right,
    const core::TypedExprPtr& condition) {
  if (condition && !condition->type()->isBoolean()) {
    throw std::runtime_error("Join condition must have BOOLEAN type");
  }
  const auto output = canonicalJoinOutput(kind, left->outputType(), right->outputType());
  const bool rightSide = kind.starts_with("JOIN_TYPE_RIGHT");
  const bool mark = kind.ends_with("_MARK");
  const bool semi = kind.ends_with("_SEMI") || kind.ends_with("_ANTI");
  const bool single = kind.ends_with("_SINGLE");
  if (semi || mark || (single && rightSide)) {
    auto probe = rightSide ? right : left;
    auto build = rightSide ? left : right;
    auto markerType = canonicalJoinOutput("JOIN_TYPE_LEFT_MARK",
        probe->outputType(), build->outputType());
    if (mark) {
      // MARK is a three-valued predicate reduction, not a WHERE filter over
      // dummy hash keys. The sole SDK probe owns TRUE > UNKNOWN > FALSE state.
      return std::make_shared<core::NestedLoopJoinNode>(nextNodeId(),
          core::JoinType::kLeftSemiProject, condition, probe, build, output, true);
    }
    if (semi) {
      auto joined = std::make_shared<core::NestedLoopJoinNode>(nextNodeId(),
          core::JoinType::kLeftSemiProject, condition, probe, build, markerType);
      return finishJoinMarker(joined, output, kind.ends_with("_ANTI"));
    }
    auto joined = std::make_shared<core::NestedLoopJoinNode>(nextNodeId(),
        core::JoinType::kLeft, condition, probe, build, output);
    single_match_join_requirements_.push_back({joined->id(), false});
    return joined;
  }
  auto joined = std::make_shared<core::NestedLoopJoinNode>(nextNodeId(),
      canonicalJoinType(kind), condition, left, right, output);
  if (single) single_match_join_requirements_.push_back({joined->id(), false});
  return joined;
}

core::TypedExprPtr VeloxPlanConverter::convertJoinComparison(
    const substrait::ComparisonJoinKey::ComparisonType& comparison,
    core::TypedExprPtr left, core::TypedExprPtr right) {
  if (!left->type()->equivalent(*right->type())) {
    throw std::runtime_error("Join keys require matching declared physical types");
  }
  if (comparison.has_custom_function_reference()) {
    const auto found = function_map_.find(comparison.custom_function_reference());
    if (found == function_map_.end()) {
      throw std::runtime_error("Join comparison references an unbound function");
    }
    return checkedScalarCall(found->second, BOOLEAN(), {left, right});
  }
  if (!comparison.has_simple()) throw std::runtime_error("Join key has no comparison");
  auto equality = checkedScalarCall("eq", BOOLEAN(), {left, right});
  switch (comparison.simple()) {
    case substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ:
      return equality;
    case substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_IS_NOT_DISTINCT_FROM:
      return checkedScalarCall("not", BOOLEAN(), {
          checkedScalarCall("distinct_from", BOOLEAN(), {left, right})});
    case substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_MIGHT_EQUAL:
      return checkedScalarCall("or", BOOLEAN(), {equality,
          checkedScalarCall("is_null", BOOLEAN(), {equality})});
    default:
      throw std::runtime_error("Join key has an unspecified comparison");
  }
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::appendJoinKeys(
    std::shared_ptr<const core::PlanNode> input,
    const std::vector<core::TypedExprPtr>& expressions, size_t offset,
    std::vector<core::FieldAccessTypedExprPtr>& keys) {
  auto names = input->outputType()->names();
  std::vector<core::TypedExprPtr> projections;
  for (size_t i = 0; i < names.size(); ++i) {
    projections.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        input->outputType()->childAt(i), names[i]));
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    const auto name = physicalFieldAlias(offset + i);
    names.push_back(name);
    projections.push_back(expressions[i]);
    keys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        expressions[i]->type(), name));
  }
  return std::make_shared<core::ProjectNode>(nextNodeId(), names, projections, input);
}

VeloxPlanConverter::PreparedJoinKeys VeloxPlanConverter::prepareJoinKeys(
    const google::protobuf::RepeatedPtrField<substrait::ComparisonJoinKey>& keys,
    std::shared_ptr<const core::PlanNode> left,
    std::shared_ptr<const core::PlanNode> right) {
  if (keys.empty()) throw std::runtime_error("Physical join requires non-empty canonical keys");
  PreparedJoinKeys result;
  std::vector<core::TypedExprPtr> leftExpressions, rightExpressions;
  for (const auto& key : keys) {
    if (!key.has_left() || !key.has_right() || !key.has_comparison()) {
      throw std::runtime_error("Physical join requires both keys and their comparison");
    }
    auto leftExpression = convertFieldReference(key.left(), left->outputType());
    auto rightExpression = convertFieldReference(key.right(), right->outputType());
    auto predicate = convertJoinComparison(key.comparison(), leftExpression, rightExpression);
    result.condition = result.condition
        ? checkedScalarCall("and", BOOLEAN(), {result.condition, predicate}) : predicate;
    const auto mode = key.comparison().has_simple()
        ? key.comparison().simple()
        : substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_UNSPECIFIED;
    result.ordinaryEquality &= mode == substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ;
    result.hashable &= mode == substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ ||
        mode == substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_IS_NOT_DISTINCT_FROM;
    leftExpressions.push_back(leftExpression);
    rightExpressions.push_back(rightExpression);
  }
  const auto offset = left->outputType()->size() + right->outputType()->size();
  result.left = appendJoinKeys(left, leftExpressions, offset, result.leftKeys);
  result.right = appendJoinKeys(right, rightExpressions, offset + keys.size(), result.rightKeys);
  return result;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::buildKeyedJoin(
    const std::string& kind, PreparedJoinKeys keys, const RowTypePtr& outputType,
    bool buildLeft, bool merge) {
  auto type = canonicalJoinType(kind);
  const bool rightAnti = kind == "JOIN_TYPE_RIGHT_ANTI";
  if (buildLeft) {
    std::swap(keys.left, keys.right);
    std::swap(keys.leftKeys, keys.rightKeys);
    type = rightAnti ? core::JoinType::kAnti : reverseJoinType(type);
  }
  const bool marker = type == core::JoinType::kRightSemiProject && !kind.ends_with("_MARK");
  auto actualOutput = outputType;
  if (marker) {
    auto names = outputType->names();
    auto types = outputType->children();
    names.push_back(physicalFieldAlias(keys.left->outputType()->size() +
        keys.right->outputType()->size()));
    types.push_back(BOOLEAN());
    actualOutput = ROW(std::move(names), std::move(types));
  }
  std::shared_ptr<const core::PlanNode> joined;
  if (merge && keys.ordinaryEquality && core::MergeJoinNode::isSupported(type)) {
    const std::vector<core::SortOrder> orders(keys.leftKeys.size(), {true, true});
    core::PlanNodePtr left = keys.left;
    core::PlanNodePtr right = keys.right;
    if (!hasExactMergeOrder(left, keys.leftKeys)) {
      left = std::make_shared<core::OrderByNode>(nextNodeId(), keys.leftKeys, orders, false, left);
    }
    if (!hasExactMergeOrder(right, keys.rightKeys)) {
      right = std::make_shared<core::OrderByNode>(nextNodeId(), keys.rightKeys, orders, false, right);
    }
    joined = std::make_shared<core::MergeJoinNode>(nextNodeId(), type,
        keys.leftKeys, keys.rightKeys, keys.condition, left, right, actualOutput);
  } else {
    joined = std::make_shared<core::HashJoinNode>(nextNodeId(), type, false,
        keys.leftKeys, keys.rightKeys, keys.condition, keys.left, keys.right,
        actualOutput, false, !keys.ordinaryEquality);
  }
  if (kind.ends_with("_SINGLE")) {
    single_match_join_requirements_.push_back({joined->id(),
        type == core::JoinType::kRight && !merge});
  }
  return marker ? finishJoinMarker(joined, outputType, true) : joined;
}

}  // namespace pangrova::velox
