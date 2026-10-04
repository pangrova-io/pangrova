// Exact aggregate arguments and grouping ordinals over one actual projection.
#include "velox/plan/VeloxPlanConverter.h"

#include <algorithm>

#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "capability/RegisteredFunctionBinding.h"

namespace pangrova::velox {
using namespace facebook::velox;

std::vector<core::FieldAccessTypedExprPtr> VeloxPlanConverter::convertGroupingKeys(
    const substrait::AggregateRel& relation, const RowTypePtr& preparedType,
    size_t baseArity) {
  std::unordered_set<uint32_t> references;
  for (const auto& grouping : relation.groupings()) {
    for (const auto reference : grouping.expression_references()) {
      if (reference >= static_cast<uint32_t>(relation.grouping_expressions_size())) {
        throw std::runtime_error("Aggregate grouping expression reference is out of bounds");
      }
      references.emplace(reference);
    }
  }
  if (references.size() != static_cast<size_t>(relation.grouping_expressions_size())) {
    throw std::runtime_error("Every grouping expression must be referenced");
  }
  std::vector<core::FieldAccessTypedExprPtr> keys;
  for (int i = 0; i < relation.grouping_expressions_size(); ++i) {
    keys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        preparedType->childAt(baseArity + i), preparedType->nameOf(baseArity + i)));
  }
  return keys;
}

core::TypedExprPtr VeloxPlanConverter::aggregateInputValue(
    const substrait::Expression& expression, const RowTypePtr& inputType,
    std::vector<std::string>& names, std::vector<core::TypedExprPtr>& projections,
    bool preserveConstant) {
  auto value = convertExpression(expression, inputType);
  auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(value);
  if (field && field->inputs().empty()) return field;
  if (preserveConstant && std::dynamic_pointer_cast<const core::ConstantTypedExpr>(value)) return value;
  const auto name = physicalFieldAlias(names.size());
  names.push_back(name);
  projections.push_back(value);
  return std::make_shared<core::FieldAccessTypedExpr>(value->type(), name);
}

core::AggregationNode::Aggregate VeloxPlanConverter::convertAggregateMeasure(
    const substrait::AggregateRel::Measure& measure, const RowTypePtr& inputType,
    std::vector<std::string>& names, std::vector<core::TypedExprPtr>& projections) {
  if (!measure.has_measure()) throw std::runtime_error("Aggregate measure has no function");
  const auto& function = measure.measure();
  requireFunctionKind(function.function_reference(), protocol::v2::FUNCTION_KIND_AGGREGATE);
  const auto deprecated = function.GetDescriptor()->FindFieldByName("args");
  if (function.GetReflection()->FieldSize(function, deprecated) != 0) {
    throw std::runtime_error("Deprecated aggregate args are not a canonical plan");
  }
  const auto found = function_map_.find(function.function_reference());
  if (found == function_map_.end()) throw std::runtime_error("Aggregate references an unbound function");
  std::vector<core::TypedExprPtr> arguments;
  core::AggregationNode::Aggregate aggregate;
  for (const auto& argument : function.arguments()) {
    if (!argument.has_value()) throw std::runtime_error("Aggregate requires canonical value arguments");
    auto value = aggregateInputValue(argument.value(), inputType, names, projections, true);
    aggregate.rawInputTypes.push_back(value->type());
    arguments.push_back(std::move(value));
  }
  if (function.phase() == substrait::AGGREGATION_PHASE_UNSPECIFIED ||
      function.phase() == substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE ||
      function.phase() == substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT) {
    throw std::runtime_error("State aggregate phase requires authoritative original raw-input types");
  }
  aggregate.call = pangrova::capability::bindRegisteredRawAggregate(*function_registry_, found->second,
      convertType(function.output_type()), std::move(arguments),
      function.phase() == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE).call;
  aggregate.distinct = function.invocation() == substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT;
  if (measure.has_filter()) {
    if (function.phase() == substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE ||
        function.phase() == substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT) {
      throw std::runtime_error("FILTER masks are only admitted on raw single/partial aggregate phases");
    }
    auto mask = aggregateInputValue(measure.filter(), inputType, names, projections, false);
    if (!mask->type()->isBoolean()) throw std::runtime_error("Aggregate FILTER must have BOOLEAN type");
    aggregate.mask = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(mask);
  }
  for (const auto& sort : function.sorts()) {
    if (!sort.has_expr()) throw std::runtime_error("Aggregate sort requires an expression");
    auto key = aggregateInputValue(sort.expr(), inputType, names, projections, false);
    aggregate.sortingKeys.push_back(std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(key));
    aggregate.sortingOrders.push_back(convertSortOrder(sort));
  }
  return aggregate;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertAggregateMeasures(
    const substrait::AggregateRel& relation, std::shared_ptr<const core::PlanNode> input,
    const RowTypePtr& inputType, std::vector<std::string>& aggregateNames,
    std::vector<core::AggregationNode::Aggregate>& aggregates) {
  if (relation.groupings_size() == 0 && relation.measures_size() == 0) {
    throw std::runtime_error("Aggregate requires grouping sets or measures");
  }
  auto names = inputType->names();
  std::vector<core::TypedExprPtr> projections;
  for (size_t i = 0; i < inputType->size(); ++i) {
    projections.push_back(std::make_shared<core::FieldAccessTypedExpr>(inputType->childAt(i), names[i]));
  }
  for (const auto& expression : relation.grouping_expressions()) {
    names.push_back(physicalFieldAlias(names.size()));
    projections.push_back(convertExpression(expression, inputType));
  }
  for (const auto& measure : relation.measures()) {
    aggregates.push_back(convertAggregateMeasure(measure, inputType, names, projections));
  }
  for (int i = 0; i < relation.measures_size(); ++i) {
    aggregateNames.push_back(physicalFieldAlias(names.size() + relation.grouping_expressions_size() +
        (relation.groupings_size() > 1 ? 1 : 0) + i));
  }
  return std::make_shared<core::ProjectNode>(nextNodeId(), std::move(names), std::move(projections), input);
}

core::AggregationNode::Step VeloxPlanConverter::resolveAggregationStep(
    const substrait::AggregateRel& relation) {
  if (relation.measures_size() == 0) return core::AggregationNode::Step::kSingle;
  const auto phase = relation.measures(0).measure().phase();
  for (const auto& measure : relation.measures()) {
    if (!measure.has_measure() || measure.measure().phase() != phase) {
      throw std::runtime_error("All aggregate measures must have the same declared phase");
    }
  }
  switch (phase) {
    case substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE: return core::AggregationNode::Step::kPartial;
    case substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE: return core::AggregationNode::Step::kIntermediate;
    case substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT: return core::AggregationNode::Step::kSingle;
    case substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT: return core::AggregationNode::Step::kFinal;
    case substrait::AGGREGATION_PHASE_UNSPECIFIED: return core::AggregationNode::Step::kFinal;
    default: throw std::runtime_error("Aggregate has an unknown phase");
  }
}

std::vector<core::FieldAccessTypedExprPtr> VeloxPlanConverter::detectPreGroupedKeys(
    const substrait::AggregateRel& relation, const std::shared_ptr<const core::PlanNode>& input,
    const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys) {
  const auto sort = std::dynamic_pointer_cast<const core::OrderByNode>(input);
  if (!sort || sort->sortingKeys().size() < groupingKeys.size()) return {};
  for (size_t i = 0; i < groupingKeys.size(); ++i) {
    const auto key = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
        convertExpression(relation.grouping_expressions(i), input->outputType()));
    if (!key || !key->inputs().empty() || key->name() != sort->sortingKeys()[i]->name()) return {};
  }
  return groupingKeys;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertAggregateRel(
    const substrait::AggregateRel& relation, const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(relation.input(), splits);
  const auto inputType = input->outputType();
  if (relation.groupings_size() > 1) return convertMultiGroupingAggregate(relation, input, inputType);
  std::vector<std::string> names;
  std::vector<core::AggregationNode::Aggregate> aggregates;
  auto prepared = convertAggregateMeasures(relation, input, inputType, names, aggregates);
  const auto keys = convertGroupingKeys(relation, prepared->outputType(), inputType->size());
  const auto preGrouped = detectPreGroupedKeys(relation, input, keys);
  const auto step = resolveAggregationStep(relation);
  const bool requiresSingle = std::any_of(aggregates.begin(), aggregates.end(),
      [](const auto& aggregate) { return aggregate.distinct || !aggregate.sortingKeys.empty(); });
  if (step == core::AggregationNode::Step::kSingle && !requiresSingle) {
    return buildParallelAggregation(keys, preGrouped, names, aggregates, prepared);
  }
  if (step == core::AggregationNode::Step::kSingle && requiresSingle) {
    prepared = insertLocalPartitionExchange(prepared, keys);
  }
  return std::make_shared<core::AggregationNode>(nextNodeId(), step, keys,
      step == core::AggregationNode::Step::kSingle && requiresSingle
          ? std::vector<core::FieldAccessTypedExprPtr>{} : preGrouped,
      names, aggregates, false, false, prepared);
}

}  // namespace pangrova::velox
