// Canonical set algebra: DISTINCT and ALL are not interchangeable.
#include "velox/plan/VeloxPlanConverter.h"

#include "velox/plan/CanonicalOrdinalIdentity.h"

namespace pangrova::velox {
using namespace facebook::velox;

namespace {
void requireSetShape(const RowTypePtr& expected, const RowTypePtr& actual) {
  if (expected->size() != actual->size()) throw std::runtime_error("Set input arity mismatch");
  for (size_t i = 0; i < expected->size(); ++i) {
    if (!expected->childAt(i)->equivalent(*actual->childAt(i))) {
      throw std::runtime_error("Set input physical type mismatch");
    }
  }
}
}  // namespace

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::buildUnionAll(
    std::vector<std::shared_ptr<const core::PlanNode>> sources) {
  const auto type = sources.front()->outputType();
  for (auto& source : sources) {
    requireSetShape(type, source->outputType());
    source = rebaseJoinInput(source, 0);
  }
  return core::LocalPartitionNode::gather(nextNodeId(), std::move(sources));
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::buildIntersectOrExcept(
    std::vector<std::shared_ptr<const core::PlanNode>> sources, core::JoinType joinType) {
  auto result = sources.front();
  for (size_t s = 1; s < sources.size(); ++s) {
    const auto output = result->outputType();
    requireSetShape(output, sources[s]->outputType());
    auto left = result;
    auto right = rebaseJoinInput(sources[s], output->size());
    std::vector<core::FieldAccessTypedExprPtr> leftKeys, rightKeys;
    if (output->size() == 0) {
      auto zero = std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(int64_t{0}));
      left = appendJoinKeys(left, {zero}, 0, leftKeys);
      right = appendJoinKeys(right, {zero}, 1, rightKeys);
    } else {
      for (size_t i = 0; i < output->size(); ++i) {
        leftKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
            output->childAt(i), left->outputType()->nameOf(i)));
        rightKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
            output->childAt(i), right->outputType()->nameOf(i)));
      }
    }
    result = std::make_shared<core::HashJoinNode>(nextNodeId(), joinType, false,
        leftKeys, rightKeys, nullptr, left, right, output, false, true);
  }
  return result;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::deduplicateSet(
    std::shared_ptr<const core::PlanNode> input) {
  const auto output = input->outputType();
  std::vector<core::FieldAccessTypedExprPtr> keys;
  if (output->size() == 0) {
    auto zero = std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(int64_t{0}));
    input = appendJoinKeys(input, {zero}, 0, keys);
  } else {
    for (size_t i = 0; i < output->size(); ++i) {
      keys.push_back(std::make_shared<core::FieldAccessTypedExpr>(output->childAt(i), output->nameOf(i)));
    }
  }
  auto gathered = core::LocalPartitionNode::gather(nextNodeId(), {input});
  auto distinct = std::make_shared<core::AggregationNode>(nextNodeId(),
      core::AggregationNode::Step::kSingle, keys,
      std::vector<core::FieldAccessTypedExprPtr>{}, std::vector<std::string>{},
      std::vector<core::AggregationNode::Aggregate>{}, false, false, gathered);
  if (output->size() != 0) return distinct;
  // A constant grouping key produces one group only for non-empty input;
  // a global zero-key aggregate would incorrectly invent a row for empty input.
  return std::make_shared<core::ProjectNode>(nextNodeId(), std::vector<std::string>{},
      std::vector<core::TypedExprPtr>{}, distinct);
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertSetRel(
    const substrait::SetRel& relation, const std::vector<split::SplitConfig>& splits) {
  if (relation.inputs_size() < 2) throw std::runtime_error("Set requires at least two inputs");
  std::vector<std::shared_ptr<const core::PlanNode>> sources;
  for (const auto& input : relation.inputs()) sources.push_back(convertRel(input, splits));
  for (const auto& source : sources) requireSetShape(sources.front()->outputType(), source->outputType());
  switch (relation.op()) {
    case substrait::SetRel::SET_OP_UNION_ALL:
      return buildUnionAll(std::move(sources));
    case substrait::SetRel::SET_OP_UNION_DISTINCT:
      return deduplicateSet(buildUnionAll(std::move(sources)));
    case substrait::SetRel::SET_OP_MINUS_PRIMARY_ALL:
      return buildIntersectOrExcept(std::move(sources), core::JoinType::kCountingAnti);
    case substrait::SetRel::SET_OP_INTERSECTION_MULTISET_ALL:
      return buildIntersectOrExcept(std::move(sources), core::JoinType::kCountingLeftSemiFilter);
    case substrait::SetRel::SET_OP_INTERSECTION_MULTISET:
      return deduplicateSet(buildIntersectOrExcept(std::move(sources), core::JoinType::kLeftSemiFilter));
    case substrait::SetRel::SET_OP_MINUS_PRIMARY: {
      auto primary = sources.front();
      sources.erase(sources.begin());
      auto secondary = sources.size() == 1 ? sources.front() : buildUnionAll(std::move(sources));
      return deduplicateSet(buildIntersectOrExcept({primary, secondary}, core::JoinType::kAnti));
    }
    case substrait::SetRel::SET_OP_INTERSECTION_PRIMARY: {
      auto primary = sources.front();
      sources.erase(sources.begin());
      auto secondary = sources.size() == 1 ? sources.front() : buildUnionAll(std::move(sources));
      return deduplicateSet(buildIntersectOrExcept({primary, secondary}, core::JoinType::kLeftSemiFilter));
    }
    case substrait::SetRel::SET_OP_MINUS_MULTISET: {
      auto primary = sources.front();
      sources.erase(sources.begin());
      auto secondary = sources.size() == 1 ? sources.front()
          : buildIntersectOrExcept(std::move(sources), core::JoinType::kLeftSemiFilter);
      return buildIntersectOrExcept({primary, secondary}, core::JoinType::kAnti);
    }
    default:
      throw std::runtime_error("Set requires a canonical operation");
  }
}
}  // namespace pangrova::velox
