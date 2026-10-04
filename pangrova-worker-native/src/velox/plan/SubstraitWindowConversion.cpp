// One typed window-expression preparation for both canonical relation shapes.
#include "velox/plan/VeloxPlanConverter.h"

#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "capability/RegisteredFunctionBinding.h"
#include "capability/RegisteredWindowOptions.h"
#include "velox/window/DistinctAggregateWindow.h"
#include "velox/window/WindowRangeBounds.h"

#include <algorithm>

namespace pangrova::velox {
using namespace facebook::velox;
namespace {
core::WindowNode::BoundType boundType(const substrait::Expression_WindowFunction_Bound& bound,
                                     bool lower) {
  if (bound.has_unbounded() || bound.kind_case() == substrait::Expression_WindowFunction_Bound::KIND_NOT_SET) {
    return lower ? core::WindowNode::BoundType::kUnboundedPreceding
                 : core::WindowNode::BoundType::kUnboundedFollowing;
  }
  if (bound.has_current_row()) return core::WindowNode::BoundType::kCurrentRow;
  if (bound.has_preceding()) return core::WindowNode::BoundType::kPreceding;
  if (bound.has_following()) return core::WindowNode::BoundType::kFollowing;
  throw std::runtime_error("Window has an invalid bound kind");
}

core::ConstantTypedExprPtr boundValue(const substrait::Expression_WindowFunction_Bound& bound) {
  if (!bound.has_preceding() && !bound.has_following()) return nullptr;
  const auto offset = bound.has_preceding() ? bound.preceding().offset() : bound.following().offset();
  if (offset <= 0) throw std::runtime_error("Window bound offset must be strictly positive");
  return std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(offset));
}

bool offsetRange(const substrait::Expression_WindowFunction& function) {
  return function.bounds_type() == substrait::Expression_WindowFunction::BOUNDS_TYPE_RANGE &&
      (function.lower_bound().has_preceding() || function.lower_bound().has_following() ||
       function.upper_bound().has_preceding() || function.upper_bound().has_following());
}

core::TypedExprPtr projectedValue(const core::FieldAccessTypedExprPtr& field,
    const std::vector<std::string>& names, const std::vector<core::TypedExprPtr>& projections) {
  const auto found = std::find(names.begin(), names.end(), field->name());
  if (found == names.end()) throw std::runtime_error("Window key is absent from its preparation projection");
  return projections.at(std::distance(names.begin(), found));
}

core::FieldAccessTypedExprPtr appendWindowValue(core::TypedExprPtr value,
    std::vector<std::string>& names, std::vector<core::TypedExprPtr>& projections) {
  auto name = physicalFieldAlias(names.size());
  auto result = std::make_shared<core::FieldAccessTypedExpr>(value->type(), name);
  names.push_back(std::move(name));
  projections.push_back(std::move(value));
  return result;
}

core::TypedExprPtr preparedBound(const substrait::Expression_WindowFunction_Bound& bound,
    const exec::FunctionRegistrySnapshot& registry,
    const core::WindowNode::WindowType type, const std::vector<core::FieldAccessTypedExprPtr>& sorts,
    const std::vector<core::SortOrder>& orders, std::vector<std::string>& names,
    std::vector<core::TypedExprPtr>& projections) {
  auto offset = boundValue(bound);
  if (!offset || type == core::WindowNode::WindowType::kRows) return offset;
  if (sorts.size() != 1 || orders.size() != 1) {
    throw std::runtime_error("Bounded RANGE requires exactly one ordering key");
  }
  auto comparison = projectedValue(sorts.front(), names, projections);
  const auto subtract = bound.has_preceding() == orders.front().isAscending();
  return appendWindowValue(rangeWindowBoundary(registry, std::move(comparison), offset->value().value<int64_t>(), subtract),
      names, projections);
}

bool rowNumberOnly(const substrait::Expression_WindowFunction& function,
                   const std::unordered_map<uint32_t, std::string>& functions) {
  const auto found = functions.find(function.function_reference());
  return found != functions.end() && found->second == "row_number" &&
      function.arguments_size() == 0 && function.options_size() == 0 &&
      function.output_type().has_i64() &&
      (!function.has_lower_bound() || function.lower_bound().has_unbounded()) &&
      (!function.has_upper_bound() || function.upper_bound().has_current_row() ||
          function.upper_bound().has_unbounded());
}
}  // namespace

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertWindowRel(
    const substrait::ConsistentPartitionWindowRel& relation,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(relation.input(), splits);
  substrait::ProjectRel project;
  for (const auto& source : relation.window_functions()) {
    auto* target = project.add_expressions()->mutable_window_function();
    target->set_function_reference(source.function_reference());
    target->mutable_arguments()->CopyFrom(source.arguments());
    target->mutable_options()->CopyFrom(source.options());
    target->mutable_output_type()->CopyFrom(source.output_type());
    target->set_phase(source.phase());
    target->set_invocation(source.invocation());
    target->set_bounds_type(source.bounds_type());
    if (source.has_lower_bound()) target->mutable_lower_bound()->CopyFrom(source.lower_bound());
    if (source.has_upper_bound()) target->mutable_upper_bound()->CopyFrom(source.upper_bound());
    target->mutable_partitions()->CopyFrom(relation.partition_expressions());
    target->mutable_sorts()->CopyFrom(relation.sorts());
  }
  return convertProjectWithWindowFunctions(project, input, input->outputType());
}

core::WindowNode::Function VeloxPlanConverter::convertWindowFunction(
    const substrait::Expression_WindowFunction& function, const RowTypePtr& expressionType,
    std::vector<std::string>& names, std::vector<core::TypedExprPtr>& projections,
    const std::vector<core::FieldAccessTypedExprPtr>& sorts, const std::vector<core::SortOrder>& orders) {
  requireFunctionKind(function.function_reference(), protocol::v2::FUNCTION_KIND_WINDOW);
  const auto deprecated = function.GetDescriptor()->FindFieldByName("args");
  if (function.GetReflection()->FieldSize(function, deprecated) != 0) {
    throw std::runtime_error("Deprecated window args are not a canonical plan");
  }
  const auto found = function_map_.find(function.function_reference());
  if (found == function_map_.end()) throw std::runtime_error("Window references an unbound function");
  if (function.phase() != substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT) {
    throw std::runtime_error("Window execution requires INITIAL_TO_RESULT with actual raw input types");
  }
  if (!substrait::AggregateFunction_AggregationInvocation_IsValid(function.invocation())) {
    throw std::runtime_error("Window has an unknown aggregation invocation");
  }
  if (function.bounds_type() != substrait::Expression_WindowFunction::BOUNDS_TYPE_ROWS &&
      function.bounds_type() != substrait::Expression_WindowFunction::BOUNDS_TYPE_RANGE) {
    throw std::runtime_error("Window requires an explicit valid bounds type");
  }
  std::vector<core::TypedExprPtr> arguments;
  for (const auto& argument : function.arguments()) {
    if (!argument.has_value()) throw std::runtime_error("Window requires canonical value arguments");
    arguments.push_back(aggregateInputValue(argument.value(), expressionType, names, projections, true));
  }
  core::WindowNode::Function result;
  const auto name = function.invocation() == substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT
      ? distinctAggregateWindowName(found->second) : found->second;
  result.functionCall = pangrova::capability::bindRegisteredWindow(
      *function_registry_, name, convertType(function.output_type()), std::move(arguments));
  result.frame.type = function.bounds_type() == substrait::Expression_WindowFunction::BOUNDS_TYPE_RANGE
      ? core::WindowNode::WindowType::kRange : core::WindowNode::WindowType::kRows;
  result.frame.startType = boundType(function.lower_bound(), true);
  result.frame.endType = boundType(function.upper_bound(), false);
  result.frame.startValue = preparedBound(function.lower_bound(), *function_registry_, result.frame.type, sorts, orders, names, projections);
  result.frame.endValue = preparedBound(function.upper_bound(), *function_registry_, result.frame.type, sorts, orders, names, projections);
  result.ignoreNulls = pangrova::capability::registeredWindowIgnoreNulls(found->second, function.options());
  return result;
}

VeloxPlanConverter::PreparedWindow VeloxPlanConverter::prepareWindowGroup(
    const WindowGroup& group, const RowTypePtr& expressionType, core::PlanNodePtr input) {
  for (const auto& function : group.funcs) {
    requireFunctionKind(function.wf->function_reference(), protocol::v2::FUNCTION_KIND_WINDOW);
  }
  PreparedWindow result;
  auto names = input->outputType()->names();
  std::vector<core::TypedExprPtr> projections;
  for (size_t i = 0; i < names.size(); ++i) {
    projections.push_back(std::make_shared<core::FieldAccessTypedExpr>(input->outputType()->childAt(i), names[i]));
  }
  for (const auto& expression : group.funcs.front().wf->partitions()) {
    result.partitions.push_back(std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
        aggregateInputValue(expression, expressionType, names, projections, false)));
  }
  for (const auto& sort : group.funcs.front().wf->sorts()) {
    if (!sort.has_expr()) throw std::runtime_error("Window sort requires an expression");
    result.sorts.push_back(std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
        aggregateInputValue(sort.expr(), expressionType, names, projections, false)));
    result.orders.push_back(convertSortOrder(sort));
  }
  if (std::any_of(group.funcs.begin(), group.funcs.end(), [](const auto& function) { return offsetRange(*function.wf); })) {
    if (result.sorts.size() != 1) throw std::runtime_error("Bounded RANGE requires exactly one ordering key");
    auto value = projectedValue(result.sorts.front(), names, projections);
    result.sorts.front() = appendWindowValue(rangeWindowComparison(std::move(value)), names, projections);
  }
  for (const auto& function : group.funcs) {
    result.functions.push_back(convertWindowFunction(*function.wf, expressionType, names, projections, result.sorts, result.orders));
  }
  for (size_t i = 0; i < group.funcs.size(); ++i) result.names.push_back(physicalFieldAlias(names.size() + i));
  result.input = std::make_shared<core::ProjectNode>(nextNodeId(), std::move(names), std::move(projections), input);
  return result;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::buildWindowNode(
    const WindowGroup& group, const RowTypePtr& expressionType, core::PlanNodePtr input,
    std::unordered_map<int, std::string>& outputColumns) {
  auto prepared = prepareWindowGroup(group, expressionType, input);
  for (size_t i = 0; i < group.funcs.size(); ++i) outputColumns[group.funcs[i].proj_index] = prepared.names[i];
  if (group.funcs.size() == 1 && rowNumberOnly(*group.funcs.front().wf, function_map_)) {
    auto source = insertLocalPartitionExchange(prepared.input, prepared.partitions);
    if (!prepared.sorts.empty()) {
      auto keys = prepared.partitions;
      std::vector<core::SortOrder> orders(keys.size(), core::SortOrder(true, true));
      keys.insert(keys.end(), prepared.sorts.begin(), prepared.sorts.end());
      orders.insert(orders.end(), prepared.orders.begin(), prepared.orders.end());
      source = std::make_shared<core::OrderByNode>(nextNodeId(), keys, orders, false, source);
    }
    auto id = nextNodeId();
    auto result = std::make_shared<core::RowNumberNode>(id, prepared.partitions,
        std::make_optional(prepared.names.front()), std::nullopt, source);
    if (!prepared.sorts.empty()) row_number_sort_info_[id] = {prepared.sorts, prepared.orders};
    return result;
  }
  auto source = core::LocalPartitionNode::gather(nextNodeId(), {prepared.input});
  return std::make_shared<core::WindowNode>(nextNodeId(), prepared.partitions,
      prepared.sorts, prepared.orders, prepared.names, prepared.functions, false, source);
}

std::vector<VeloxPlanConverter::WindowFuncInfo> VeloxPlanConverter::extractWindowFunctions(
    const substrait::ProjectRel& relation) {
  std::vector<WindowFuncInfo> result;
  for (int i = 0; i < relation.expressions_size(); ++i) {
    if (relation.expressions(i).has_window_function()) result.push_back({i, &relation.expressions(i).window_function()});
  }
  return result;
}

std::vector<VeloxPlanConverter::WindowGroup> VeloxPlanConverter::groupWindowFunctions(
    const std::vector<WindowFuncInfo>& functions) {
  std::vector<WindowGroup> groups;
  std::unordered_map<std::string, size_t> ordinals;
  for (const auto& function : functions) {
    substrait::Expression_WindowFunction keys;
    keys.mutable_partitions()->CopyFrom(function.wf->partitions());
    keys.mutable_sorts()->CopyFrom(function.wf->sorts());
    const auto signature = keys.SerializeAsString();
    auto [position, inserted] = ordinals.try_emplace(signature, groups.size());
    if (inserted) groups.push_back({signature, {}});
    groups[position->second].funcs.push_back(function);
  }
  return groups;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertProjectWithWindowFunctions(
    const substrait::ProjectRel& relation, core::PlanNodePtr input, const RowTypePtr& inputType) {
  const auto groups = groupWindowFunctions(extractWindowFunctions(relation));
  std::unordered_map<int, std::string> outputs;
  auto current = input;
  for (const auto& group : groups) current = buildWindowNode(group, inputType, current, outputs);
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> projections;
  for (size_t i = 0; i < inputType->size(); ++i) {
    names.push_back(physicalFieldAlias(names.size()));
    projections.push_back(std::make_shared<core::FieldAccessTypedExpr>(inputType->childAt(i), inputType->nameOf(i)));
  }
  for (int i = 0; i < relation.expressions_size(); ++i) {
    const auto found = outputs.find(i);
    if (found == outputs.end()) projections.push_back(convertExpression(relation.expressions(i), inputType));
    else {
      const auto ordinal = current->outputType()->getChildIdxIfExists(found->second);
      if (!ordinal) throw std::runtime_error("Declared window output column is missing");
      projections.push_back(std::make_shared<core::FieldAccessTypedExpr>(current->outputType()->childAt(*ordinal), found->second));
    }
    names.push_back(physicalFieldAlias(names.size()));
  }
  return std::make_shared<core::ProjectNode>(nextNodeId(), std::move(names), std::move(projections), current);
}
}  // namespace pangrova::velox
