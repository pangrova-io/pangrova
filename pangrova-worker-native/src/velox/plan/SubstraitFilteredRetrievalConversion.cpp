#include "velox/plan/VeloxPlanConverter.h"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <unordered_set>

#include "artifact/ArtifactVectorSearch.h"
#include "capability/NativeCapabilityCatalog.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/connector/PangrovaVeloxConnector.h"
#include "velox/plan/ArtifactBuildOperator.h"
#include "velox/plan/FilteredArtifactRetrievalOperator.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"

namespace pangrova::velox {
namespace {

bool supportedExpression(const facebook::velox::core::TypedExprPtr& expression, bool projection) {
  using namespace facebook::velox::core;
  if (std::dynamic_pointer_cast<const ConstantTypedExpr>(expression) ||
      std::dynamic_pointer_cast<const FieldAccessTypedExpr>(expression)) {
    return true;
  }
  if (!std::dynamic_pointer_cast<const CastTypedExpr>(expression)) {
    const auto call = std::dynamic_pointer_cast<const CallTypedExpr>(expression);
    static const std::unordered_set<std::string> names{
        "and", "or", "not", "eq", "neq", "lt", "lte", "gt", "gte",
        "is_null", "is_not_null", "is_true", "is_false", "if", "switch"};
    if (!call || (!names.contains(call->name()) &&
        !(projection && (call->name() == "l2_distance" || call->name() == "array_constructor")))) {
      return false;
    }
  }
  return std::all_of(expression->inputs().begin(), expression->inputs().end(),
      [&](const auto& input) { return supportedExpression(input, projection); });
}

void collectFields(const facebook::velox::core::TypedExprPtr& expression,
    std::unordered_set<std::string>& fields) {
  if (auto field = std::dynamic_pointer_cast<const facebook::velox::core::FieldAccessTypedExpr>(expression)) {
    fields.insert(field->name());
  }
  for (const auto& input : expression->inputs()) {
    collectFields(input, fields);
  }
}

uint64_t searchList(const protocol::v2::CapabilityInvocation& invocation) {
  const protocol::v2::NamedValue* parameter = nullptr;
  const protocol::v2::NamedValue* metric = nullptr;
  for (const auto& option : invocation.options()) {
    if (option.name() == "search_list_size") {
      if (parameter) throw std::runtime_error("Filtered ANN graph work option is duplicated");
      parameter = &option;
    }
    if (option.name() == "metric") {
      if (metric) throw std::runtime_error("Filtered ANN metric option is duplicated");
      metric = &option;
    }
  }
  if (!parameter || !parameter->value().has_int64_value() ||
      parameter->value().int64_value() <= 0 || parameter->value().int64_value() > 100'000 ||
      !metric || !metric->value().has_string_value() || metric->value().string_value() != "L2") {
    throw std::runtime_error("Filtered ANN options require bounded graph work and L2");
  }
  return static_cast<uint64_t>(parameter->value().int64_value());
}

struct FilteredInput {
  std::vector<std::shared_ptr<const facebook::velox::core::ProjectNode>> projects;
  std::vector<std::shared_ptr<const facebook::velox::core::FilterNode>> filters;
  std::shared_ptr<const facebook::velox::core::TableScanNode> scan;
  std::unordered_set<std::string> required_fields;
  std::vector<std::string> identity_aliases;
};

FilteredInput inspectInput(facebook::velox::core::PlanNodePtr input) {
  using namespace facebook::velox;
  FilteredInput result;
  while (auto project = std::dynamic_pointer_cast<const core::ProjectNode>(input)) {
    for (const auto& expression : project->projections()) {
      if (!supportedExpression(expression, true)) {
        throw std::runtime_error("Filtered ANN projected expression is outside its typed scope");
      }
    }
    result.projects.push_back(project);
    input = project->sources().front();
  }
  while (auto filter = std::dynamic_pointer_cast<const core::FilterNode>(input)) {
    if (!supportedExpression(filter->filter(), false)) {
      throw std::runtime_error("Filtered ANN predicate is outside its typed scope");
    }
    collectFields(filter->filter(), result.required_fields);
    result.filters.push_back(filter);
    input = filter->sources().front();
  }
  result.scan = std::dynamic_pointer_cast<const core::TableScanNode>(input);
  if (!result.scan || result.filters.empty()) {
    throw std::runtime_error("Filtered ANN requires a table scan and its actual typed filter");
  }
  return result;
}

void resolveFields(FilteredInput& inspected, const std::vector<split::SplitConfig>& splits) {
  const auto& scan = inspected.scan;
  auto& required_fields = inspected.required_fields;
  auto& identity_aliases = inspected.identity_aliases;
  for (const auto& split : splits) {
    if (!split.iceberg) continue;
    for (const auto& deletion : split.iceberg->delete_files) {
      for (const auto& key : deletion.equality_fields) {
        const auto assignment = std::find_if(scan->assignments().begin(), scan->assignments().end(),
            [&](const auto& field) { return field.second->name() == key; });
        if (assignment == scan->assignments().end()) {
          throw std::runtime_error("Filtered ANN scan cannot resolve a signed equality-delete key");
        }
        required_fields.insert(assignment->first);
      }
    }
  }
  const std::array<std::string_view, 4> identity_names{
      kSourceIdColumnName, kSourceSnapshotColumnName, kSourceObjectIdColumnName, kPhysicalRowPositionColumnName};
  for (auto name : identity_names) {
    std::string alias;
    for (const auto& [candidate, assignment] : scan->assignments()) {
      if (assignment->name() == name) {
        if (!alias.empty()) throw std::runtime_error("Filtered ANN identity assignment is duplicated");
        alias = candidate;
      }
    }
    if (alias.empty()) throw std::runtime_error("Filtered ANN physical identity assignment is absent");
    identity_aliases.push_back(alias);
    required_fields.insert(alias);
  }
}

void validateVectorProperties(const FilteredArtifactRetrievalConfig& config) {
  int64_t dimension = 0;
  bool dimension_seen = false;
  bool metric = false;
  for (const auto& property : config.artifact.properties()) {
    if (property.name() == "dimension") {
      if (dimension_seen || !property.value().has_int64_value()) {
        throw std::runtime_error("Filtered ANN immutable dimension is invalid");
      }
      dimension_seen = true;
      dimension = property.value().int64_value();
    } else if (property.name() == "metric") {
      if (metric || !property.value().has_string_value() || property.value().string_value() != "L2") {
        throw std::runtime_error("Filtered ANN immutable metric differs from its query");
      }
      metric = true;
    }
  }
  if (dimension <= 0 || static_cast<uint64_t>(dimension) != config.query.size() || !metric) {
    throw std::runtime_error("Filtered ANN immutable dimension/metric is absent or mismatched");
  }
}

}  // namespace

facebook::velox::core::PlanNodePtr VeloxPlanConverter::convertFilteredRetrievalInput(
    const protocol::v2::RetrievalSourceDetail& detail,
    const protocol::v2::OperatorBinding& binding,
    facebook::velox::core::PlanNodePtr input,
    const std::vector<split::SplitConfig>& splits) {
  using namespace facebook::velox;
  const auto logical_input_type = logicalNodeType(input);
  auto inspected = inspectInput(input);
  resolveFields(inspected, splits);
  const auto& projects = inspected.projects;
  const auto& filters = inspected.filters;
  const auto& scan = inspected.scan;
  const auto& required_fields = inspected.required_fields;
  const auto& identity_aliases = inspected.identity_aliases;
  const auto handle = std::dynamic_pointer_cast<const PangrovaTableHandle>(scan->tableHandle());
  if (!handle || !artifact_build_context_ ||
      detail.output_contract().identity_kind() != protocol::v2::CANDIDATE_IDENTITY_KIND_PHYSICAL_SOURCE_ROW) {
    throw std::runtime_error("Filtered ANN requires one signed physical source and its task context");
  }
  std::vector<std::string> eligibility_names;
  std::vector<TypePtr> eligibility_types;
  connector::ColumnHandleMap eligibility_assignments;
  for (size_t channel = 0; channel < scan->outputType()->size(); ++channel) {
    const auto& alias = scan->outputType()->nameOf(channel);
    if (required_fields.contains(alias)) {
      eligibility_names.push_back(alias);
      eligibility_types.push_back(scan->outputType()->childAt(channel));
      eligibility_assignments.emplace(alias, scan->assignments().at(alias));
    }
  }
  auto clean_handle = std::make_shared<const PangrovaTableHandle>(
      handle->connectorId(), handle->name(), scan->id(), handle->scanSchema(), nullptr, handle->readBudget());
  core::PlanNodePtr eligibility = std::make_shared<core::TableScanNode>(scan->id(),
      ROW(eligibility_names, eligibility_types), clean_handle, eligibility_assignments);
  std::vector<std::string> logical_names;
  for (const auto& alias : eligibility_names) {
    logical_names.push_back(eligibility_assignments.at(alias)->name());
  }
  scan_node_output_types_[scan->id()] = ROW(logical_names, eligibility_types);
  for (auto iterator = filters.rbegin(); iterator != filters.rend(); ++iterator) {
    eligibility = std::make_shared<core::FilterNode>(nextNodeId(), (*iterator)->filter(), eligibility);
  }
  std::vector<core::TypedExprPtr> identities;
  for (const auto& alias : identity_aliases) {
    identities.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        scan->outputType()->findChild(alias), alias));
  }
  eligibility = std::make_shared<core::ProjectNode>(nextNodeId(), identity_aliases, identities, eligibility);
  eligibility = core::LocalPartitionNode::gather(nextNodeId(), {eligibility});
  auto hydration = std::make_shared<core::TableScanNode>(nextNodeId(),
      scan->outputType(), clean_handle, scan->assignments());
  FilteredArtifactRetrievalConfig config;
  config.artifact = binding.artifacts(0);
  config.query = artifact::ArtifactVectorSearch::boundQuery(detail.invocation());
  config.output_limit = detail.output_limit();
  config.search_list_size = searchList(detail.invocation());
  config.max_work = detail.candidate_limit();
  config.max_read_bytes = artifact_build_context_->max_object_store_read_bytes;
  config.read_budget = artifact_build_context_->read_budget;
  if (!config.read_budget || handle->readBudget() != config.read_budget) {
    throw std::runtime_error("Filtered ANN scan and search lack the same task read owner");
  }
  validateVectorProperties(config);
  config.splits = splits;
  config.hydration_scan = std::move(hydration);
  core::PlanNodePtr result = std::make_shared<FilteredArtifactRetrievalNode>(
      detail.plan_node_id(), std::move(config), eligibility);
  for (auto iterator = projects.rbegin(); iterator != projects.rend(); ++iterator) {
    const auto& original = *iterator;
    result = std::make_shared<core::ProjectNode>(nextNodeId(), original->outputType()->names(),
        original->projections(), result);
  }
  if (!result->outputType()->equivalent(*input->outputType())) {
    throw std::runtime_error("Filtered ANN output differs from its logical input contract");
  }
  logical_node_types_[result->id()] = logical_input_type;
  return result;
}

}  // namespace pangrova::velox
