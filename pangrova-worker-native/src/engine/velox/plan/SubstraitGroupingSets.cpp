// One GroupId expansion, exact aggregate expressions and canonical set identity.
#include "engine/velox/plan/VeloxPlanConverter.h"

#include <stdexcept>

#include "engine/velox/plan/CanonicalOrdinalIdentity.h"

namespace pangrova::velox {
using namespace facebook::velox;

namespace {
core::FieldAccessTypedExprPtr field(const RowTypePtr& type, size_t index) {
  return std::make_shared<core::FieldAccessTypedExpr>(
      type->childAt(index), type->nameOf(index));
}
}  // namespace

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::projectGroupingOutput(
    std::shared_ptr<const core::PlanNode> input,
    size_t groupingKeyCount, size_t measureCount) {
  const auto type = input->outputType();
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> expressions;
  for (size_t i = 0; i < groupingKeyCount; ++i) {
    names.push_back(physicalFieldAlias(names.size()));
    expressions.push_back(field(type, i));
  }
  for (size_t i = 0; i < measureCount; ++i) {
    names.push_back(physicalFieldAlias(names.size()));
    expressions.push_back(field(type, groupingKeyCount + 1 + i));
  }
  // Velox's internal GroupId is BIGINT; Substrait's structural set index is
  // required INTEGER. This explicit, bounded cast does not alter decimal or
  // function signatures and remains visible in the actual Velox plan.
  names.push_back(physicalFieldAlias(names.size()));
  expressions.push_back(std::make_shared<core::CastTypedExpr>(INTEGER(),
      std::vector<core::TypedExprPtr>{field(type, groupingKeyCount)}, false));
  return std::make_shared<core::ProjectNode>(
      nextNodeId(), std::move(names), std::move(expressions), input);
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertMultiGroupingAggregate(
    const substrait::AggregateRel& aggregate,
    std::shared_ptr<const core::PlanNode> input, const RowTypePtr& inputType) {
  std::vector<std::string> names;
  std::vector<core::AggregationNode::Aggregate> measures;
  std::vector<core::FieldAccessTypedExprPtr> inputKeys;
  auto prepared = convertAggregateMeasures(aggregate, input, inputType, names, measures, inputKeys);
  const auto preparedType = prepared->outputType();
  std::vector<core::GroupIdNode::GroupingKeyInfo> keyInfos;
  std::vector<std::vector<std::string>> sets;
  std::vector<vector_size_t> globalSets;
  for (size_t index = 0; index < inputKeys.size(); ++index) {
    keyInfos.push_back({physicalFieldAlias(preparedType->size() + index), inputKeys[index]});
  }
  for (const auto& grouping : aggregate.groupings()) {
    std::vector<std::string> set;
    std::unordered_set<uint32_t> used;
    for (const auto index : grouping.expression_references()) {
      if (!used.emplace(index).second) continue;
      const auto name = physicalFieldAlias(preparedType->size() + index);
      set.push_back(name);
    }
    if (set.empty()) globalSets.push_back(static_cast<vector_size_t>(sets.size()));
    sets.push_back(std::move(set));
  }
  std::vector<core::FieldAccessTypedExprPtr> passthrough;
  for (size_t i = 0; i < preparedType->size(); ++i) passthrough.push_back(field(preparedType, i));
  auto expanded = std::make_shared<core::GroupIdNode>(nextNodeId(), std::move(sets),
      std::move(keyInfos), std::move(passthrough),
      physicalFieldAlias(preparedType->size() + inputKeys.size()), prepared);
  const auto expandedType = expanded->outputType();
  std::vector<core::FieldAccessTypedExprPtr> keys;
  for (size_t i = 0; i < inputKeys.size(); ++i) keys.push_back(field(expandedType, i));
  auto groupId = field(expandedType, expandedType->size() - 1);
  keys.push_back(groupId);
  // The one preparation projection evaluated arguments in the original
  // context. GroupId retains those fields; it never rebinds proto ordinals.
  bool requiresSingle = false;
  for (int i = 0; i < aggregate.measures_size(); ++i) {
    requiresSingle |= measures[i].distinct || !measures[i].sortingKeys.empty();
  }
  const auto step = resolveAggregationStep(aggregate);
  const std::optional<core::FieldAccessTypedExprPtr> globalGroupId =
      globalSets.empty() ? std::nullopt : std::make_optional(groupId);
  std::shared_ptr<const core::PlanNode> result;
  if (step == core::AggregationNode::Step::kSingle && !requiresSingle) {
    result = buildParallelAggregation(keys, {}, names, measures, expanded, globalSets, globalGroupId);
  } else {
    auto source = requiresSingle ? core::LocalPartitionNode::gather(nextNodeId(), {expanded})
                           : std::shared_ptr<const core::PlanNode>(expanded);
    result = std::make_shared<core::AggregationNode>(nextNodeId(), step, keys,
        std::vector<core::FieldAccessTypedExprPtr>{}, names, measures,
        globalSets, globalGroupId, false, false, source);
  }
  return projectGroupingOutput(result, inputKeys.size(), measures.size());
}

}  // namespace pangrova::velox
