// Exact aggregate arguments and grouping ordinals over one actual projection.
#include "engine/velox/plan/VeloxPlanConverter.h"

#include <algorithm>
#include <iterator>

#include "engine/velox/plan/CanonicalOrdinalIdentity.h"
#include "capabilities/capability/RegisteredFunctionBinding.h"

namespace pangrova::velox {
using namespace facebook::velox;

namespace {
using Step = core::AggregationNode::Step;

Step resolveAggregationStep(substrait::AggregationPhase phase) {
  switch (phase) {
    case substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE: return Step::kPartial;
    case substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE: return Step::kIntermediate;
    case substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT: return Step::kSingle;
    case substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT: return Step::kFinal;
    case substrait::AGGREGATION_PHASE_UNSPECIFIED: return Step::kFinal;
    default: throw std::runtime_error("Aggregate has an unknown phase");
  }
}

void validateGroupingReferences(const substrait::AggregateRel& relation) {
  std::vector<bool> referenced(relation.grouping_expressions_size());
  for (const auto& grouping : relation.groupings()) {
    for (const auto reference : grouping.expression_references()) {
      if (reference >= referenced.size()) {
        throw std::runtime_error("Aggregate grouping expression reference is out of bounds");
      }
      referenced[reference] = true;
    }
  }
  if (std::any_of(referenced.begin(), referenced.end(), [](bool used) { return !used; })) {
    throw std::runtime_error("Every grouping expression must be referenced");
  }
}

void collectGroupIdInputs(
    const core::AggregationNode::Aggregate& measure,
    std::unordered_set<std::string>& fields,
    std::vector<core::FieldAccessTypedExprPtr>& inputs) {
  auto add = [&](const core::FieldAccessTypedExprPtr& field) {
    if (field && fields.insert(field->name()).second) inputs.push_back(field);
  };
  for (const auto& argument : measure.call->inputs()) {
    add(std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(argument));
  }
  add(measure.mask);
  for (const auto& key : measure.sortingKeys) add(key);
}
}  // namespace

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

VeloxPlanConverter::BoundAggregateMeasure VeloxPlanConverter::convertAggregateMeasure(
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
  auto binding = pangrova::capability::bindRegisteredRawAggregate(*function_registry_, found->second,
      convertType(function.output_type()), std::move(arguments),
      function.phase() == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE);
  aggregate.call = std::move(binding.call);
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
  return {std::move(aggregate), std::move(binding.intermediate)};
}

VeloxPlanConverter::PreparedAggregate VeloxPlanConverter::prepareAggregate(
    const substrait::AggregateRel& relation, std::shared_ptr<const core::PlanNode> input,
    const RowTypePtr& inputType) {
  if (relation.groupings_size() == 0 && relation.measures_size() == 0) {
    throw std::runtime_error("Aggregate requires grouping sets or measures");
  }
  validateGroupingReferences(relation);
  const auto phase = relation.measures().empty() ? substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT
                                                 : relation.measures(0).measure().phase();
  PreparedAggregate prepared;
  prepared.input = std::move(input);
  prepared.step = resolveAggregationStep(phase);
  for (const auto& measure : relation.measures()) {
    const auto& function = measure.measure();
    if (!measure.has_measure() || function.phase() != phase) {
      throw std::runtime_error("All aggregate measures must have the same declared phase");
    }
    prepared.requires_single |=
        function.invocation() == substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT ||
        !function.sorts().empty();
  }
  const bool localPartial = prepared.usesLocalPartial();
  prepared.measures.reserve(relation.measures_size());
  prepared.names.reserve(relation.measures_size());
  if (localPartial) prepared.intermediate_types.reserve(relation.measures_size());

  auto names = inputType->names();
  std::vector<core::TypedExprPtr> computed;
  std::unordered_set<std::string> usedGroupingFields;
  prepared.keys.reserve(relation.grouping_expressions_size());
  for (const auto& expression : relation.grouping_expressions()) {
    auto value = convertExpression(expression, inputType);
    const auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(value);
    if (field && field->inputs().empty() && usedGroupingFields.emplace(field->name()).second) {
      prepared.keys.push_back(field);
    } else {
      names.push_back(physicalFieldAlias(names.size()));
      prepared.keys.push_back(std::make_shared<core::FieldAccessTypedExpr>(value->type(), names.back()));
      computed.push_back(std::move(value));
    }
  }
  const bool expansion = relation.groupings_size() > 1;
  std::unordered_set<std::string> expansionFields;
  for (const auto& measure : relation.measures()) {
    auto binding = convertAggregateMeasure(measure, inputType, names, computed);
    if (expansion) collectGroupIdInputs(binding.measure, expansionFields, prepared.group_id_inputs);
    prepared.measures.push_back(std::move(binding.measure));
    if (localPartial) prepared.intermediate_types.push_back(std::move(binding.intermediate_type));
  }
  if (expansion) {
    prepared.group_id_alias_offset = names.size();
    for (const auto& key : prepared.keys) expansionFields.insert(key->name());
  }
  for (int i = 0; i < relation.measures_size(); ++i) {
    prepared.names.push_back(physicalFieldAlias(names.size() + relation.grouping_expressions_size() +
        (expansion ? 1 : 0) + i));
  }
  if (computed.empty() && (!expansion || expansionFields.size() == names.size())) return prepared;

  std::vector<core::TypedExprPtr> projections;
  std::vector<std::string> projectedNames;
  const auto projectedSize = expansion ? expansionFields.size() : names.size();
  projections.reserve(projectedSize);
  projectedNames.reserve(projectedSize);
  // GroupId eagerly loads all input children. Keep only the columns used by the
  // grouping sets and measure inputs at that boundary.
  for (size_t i = 0; i < inputType->size(); ++i) {
    if (expansion && !expansionFields.contains(names[i])) continue;
    projections.push_back(std::make_shared<core::FieldAccessTypedExpr>(inputType->childAt(i), names[i]));
    projectedNames.push_back(std::move(names[i]));
  }
  projections.insert(projections.end(), std::make_move_iterator(computed.begin()),
      std::make_move_iterator(computed.end()));
  projectedNames.insert(projectedNames.end(),
      std::make_move_iterator(names.begin() + inputType->size()), std::make_move_iterator(names.end()));
  prepared.input = std::make_shared<core::ProjectNode>(
      nextNodeId(), std::move(projectedNames), std::move(projections), std::move(prepared.input));
  return prepared;
}

std::vector<core::FieldAccessTypedExprPtr> VeloxPlanConverter::detectPreGroupedKeys(
    const std::shared_ptr<const core::PlanNode>& input,
    const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys) {
  const auto sort = std::dynamic_pointer_cast<const core::OrderByNode>(input);
  if (!sort || sort->sortingKeys().size() < groupingKeys.size()) return {};
  for (size_t i = 0; i < groupingKeys.size(); ++i) {
    if (groupingKeys[i]->name() != sort->sortingKeys()[i]->name()) return {};
  }
  return groupingKeys;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertAggregateRel(
    const substrait::AggregateRel& relation, const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(relation.input(), splits);
  const auto inputType = input->outputType();
  if (relation.groupings_size() > 1) return convertMultiGroupingAggregate(relation, input, inputType);
  auto prepared = prepareAggregate(relation, input, inputType);
  auto preGrouped = detectPreGroupedKeys(input, prepared.keys);
  if (prepared.usesLocalPartial()) {
    return buildParallelAggregation(prepared.keys, preGrouped, prepared, prepared.input);
  }
  if (prepared.step == Step::kSingle && prepared.requires_single) {
    prepared.input = insertLocalPartitionExchange(prepared.input, prepared.keys);
    preGrouped.clear();
  }
  return std::make_shared<core::AggregationNode>(nextNodeId(), prepared.step, std::move(prepared.keys),
      std::move(preGrouped), std::move(prepared.names), std::move(prepared.measures),
      false, false, std::move(prepared.input));
}

}  // namespace pangrova::velox
