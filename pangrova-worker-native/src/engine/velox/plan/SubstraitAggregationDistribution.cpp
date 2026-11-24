#include "engine/velox/plan/VeloxPlanConverter.h"

#include "velox/exec/HashPartitionFunction.h"

namespace pangrova::velox {

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::insertLocalPartitionExchange(
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const std::vector<std::shared_ptr<
        const facebook::velox::core::FieldAccessTypedExpr>>& partition_keys) {
  if (partition_keys.empty()) {
    return facebook::velox::core::LocalPartitionNode::gather(
        nextNodeId(), {input});
  }
  auto inp_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());
  std::vector<facebook::velox::column_index_t> key_channels;
  for (const auto& pk : partition_keys) {
    auto idx = inp_type->getChildIdxIfExists(pk->name());
    if (!idx.has_value()) throw std::runtime_error("Partition key is not present in actual input schema");
    key_channels.push_back(static_cast<facebook::velox::column_index_t>(idx.value()));
  }
  return std::make_shared<facebook::velox::core::LocalPartitionNode>(
      nextNodeId(),
      facebook::velox::core::LocalPartitionNode::Type::kRepartition,
      false,
      std::make_shared<facebook::velox::exec::HashPartitionFunctionSpec>(
          inp_type, std::move(key_channels)),
      std::vector<std::shared_ptr<
          const facebook::velox::core::PlanNode>>{input});
}

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::buildParallelAggregation(
    const std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& grouping_keys,
    const std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& pre_grouped_keys,
    const PreparedAggregate& aggregate,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const std::vector<facebook::velox::vector_size_t>& globalGroupingSets,
    const std::optional<facebook::velox::core::FieldAccessTypedExprPtr>& groupId) {
  // Partial calls publish the accumulator types bound with the raw inputs.
  const auto& aggregates = aggregate.measures;
  std::vector<facebook::velox::core::AggregationNode::Aggregate> partialAggs;
  partialAggs.reserve(aggregates.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    auto partial = aggregates[i];
    partial.call = std::make_shared<facebook::velox::core::CallTypedExpr>(
        aggregate.intermediate_types[i], partial.call->inputs(), partial.call->name());
    partialAggs.push_back(std::move(partial));
  }
  auto partialNode = std::make_shared<facebook::velox::core::AggregationNode>(
      nextNodeId(), facebook::velox::core::AggregationNode::Step::kPartial,
      grouping_keys, pre_grouped_keys, aggregate.names, std::move(partialAggs),
      globalGroupingSets, groupId,
      false, false, input);

  auto pType = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      partialNode->outputType());
  size_t nGK = grouping_keys.size();

  std::vector<facebook::velox::core::FieldAccessTypedExprPtr> finalGK;
  for (size_t i = 0; i < nGK; ++i) {
    finalGK.push_back(std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
        pType->childAt(i), pType->nameOf(i)));
  }
  // rawInputTypes must be ORIGINAL types, not flattened intermediate children.
  std::vector<facebook::velox::core::AggregationNode::Aggregate> finalAggs;
  for (size_t i = 0; i < aggregates.size(); ++i) {
    facebook::velox::core::AggregationNode::Aggregate fa;
    size_t col = nGK + i;
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> args{
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            pType->childAt(col), pType->nameOf(col))};
    fa.call = std::make_shared<facebook::velox::core::CallTypedExpr>(
        aggregates[i].call->type(), std::move(args), aggregates[i].call->name());
    fa.rawInputTypes = aggregates[i].rawInputTypes;
    finalAggs.push_back(std::move(fa));
  }

  // Global agg: gather(). GROUP BY: repartition by keys for parallelism.
  std::shared_ptr<const facebook::velox::core::PlanNode> exchange;
  if (grouping_keys.empty()) {
    exchange = facebook::velox::core::LocalPartitionNode::gather(
        nextNodeId(), {partialNode});
  } else {
    std::vector<facebook::velox::column_index_t> ch;
    for (size_t i = 0; i < nGK; ++i)
      ch.push_back(static_cast<facebook::velox::column_index_t>(i));
    exchange = std::make_shared<facebook::velox::core::LocalPartitionNode>(
        nextNodeId(), facebook::velox::core::LocalPartitionNode::Type::kRepartition,
        false, std::make_shared<facebook::velox::exec::HashPartitionFunctionSpec>(
            pType, std::move(ch)), std::vector<std::shared_ptr<
            const facebook::velox::core::PlanNode>>{partialNode});
  }
  std::vector<facebook::velox::core::FieldAccessTypedExprPtr> noPreGrouped;
  return std::make_shared<facebook::velox::core::AggregationNode>(
      nextNodeId(), facebook::velox::core::AggregationNode::Step::kFinal,
      finalGK, noPreGrouped, aggregate.names, std::move(finalAggs), false, false, exchange);
}

}  // namespace pangrova::velox
