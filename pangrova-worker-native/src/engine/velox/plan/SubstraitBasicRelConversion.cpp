// Canonical basic relations: filters bind before masking/emit; Project appends.
#include "engine/velox/plan/VeloxPlanConverter.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "engine/velox/connector/PangrovaTableHandle.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "engine/velox/plan/ArtifactBuildOperator.h"
#include "engine/velox/plan/CanonicalOrdinalIdentity.h"

namespace pangrova::velox {
using namespace facebook::velox;

namespace {
// Returns std::nullopt if the expression is not a constant integer.
std::optional<int64_t> extractConstantInt(
    const std::shared_ptr<const facebook::velox::core::ITypedExpr>& expr) {
  auto constant = std::dynamic_pointer_cast<
      const facebook::velox::core::ConstantTypedExpr>(expr);
  if (!constant) {
    return std::nullopt;
  }
  if (constant->hasValueVector()) {
    return std::nullopt;
  }
  const auto& val = constant->value();
  if (val.isNull()) {
    return std::nullopt;
  }
  switch (val.kind()) {
    case facebook::velox::TypeKind::INTEGER:
      return static_cast<int64_t>(val.value<int32_t>());
    case facebook::velox::TypeKind::BIGINT:
      return val.value<int64_t>();
    case facebook::velox::TypeKind::SMALLINT:
      return static_cast<int64_t>(val.value<int16_t>());
    case facebook::velox::TypeKind::TINYINT:
      return static_cast<int64_t>(val.value<int8_t>());
    default:
      return std::nullopt;
  }
}

// Check if a filter expression matches the pattern `field <= K` or `field < K`
// where field is the row_number column and K is a positive integer constant.
// Returns the effective limit (K for <=, K-1 for <), or std::nullopt if no match.
std::optional<int32_t> extractTopNLimit(
    const std::shared_ptr<const facebook::velox::core::ITypedExpr>& filter_expr,
    const std::string& row_number_col_name) {
  auto call = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(filter_expr);
  if (!call || call->inputs().size() != 2) {
    return std::nullopt;
  }

  const auto& func_name = call->name();

  // Pattern 1: rn <= K  (lte)
  // Pattern 2: rn < K   (lt)
  // Pattern 3: K >= rn  (gte with reversed args)
  // Pattern 4: K > rn   (gt with reversed args)
  std::shared_ptr<const facebook::velox::core::ITypedExpr> field_expr;
  std::shared_ptr<const facebook::velox::core::ITypedExpr> const_expr;
  bool is_less_than = false;  // true for strict < (limit = K-1)

  if (func_name == "lte" || func_name == "lessthanorequal" ||
      func_name == "less_than_or_equal") {
    // rn <= K
    field_expr = call->inputs()[0];
    const_expr = call->inputs()[1];
    is_less_than = false;
  } else if (func_name == "lt" || func_name == "lessthan" ||
             func_name == "less_than") {
    // rn < K → limit = K - 1
    field_expr = call->inputs()[0];
    const_expr = call->inputs()[1];
    is_less_than = true;
  } else if (func_name == "gte" || func_name == "greaterthanorequal" ||
             func_name == "greater_than_or_equal") {
    // K >= rn (reversed)
    field_expr = call->inputs()[1];
    const_expr = call->inputs()[0];
    is_less_than = false;
  } else if (func_name == "gt" || func_name == "greaterthan" ||
             func_name == "greater_than") {
    // K > rn (reversed) → limit = K - 1
    field_expr = call->inputs()[1];
    const_expr = call->inputs()[0];
    is_less_than = true;
  } else {
    return std::nullopt;
  }

  // Check that the field side references the row_number column.
  auto field_access = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(field_expr);
  if (!field_access || field_access->name() != row_number_col_name) {
    return std::nullopt;
  }

  // Extract the constant integer value.
  auto limit_val = extractConstantInt(const_expr);
  if (!limit_val.has_value()) {
    return std::nullopt;
  }

  // Check before subtraction: INT64_MIN < rn must not overflow signed i64.
  if (*limit_val <= (is_less_than ? 1 : 0)) return std::nullopt;
  int64_t effective_limit = is_less_than
      ? (limit_val.value() - 1)
      : limit_val.value();

  // Limit must be positive and fit in int32_t.
  if (effective_limit <= 0 ||
      effective_limit > std::numeric_limits<int32_t>::max()) {
    return std::nullopt;
  }

  return static_cast<int32_t>(effective_limit);
}

struct ProjectedRowNumber {
  std::shared_ptr<const core::RowNumberNode> node;
  std::vector<std::shared_ptr<const core::ProjectNode>> projections;
  std::string marker;
};

std::optional<ProjectedRowNumber> projectedRowNumber(core::PlanNodePtr input) {
  ProjectedRowNumber result;
  const auto outer = input->outputType();
  std::vector<size_t> ordinals;
  for (size_t i = 0; i < outer->size(); ++i) ordinals.push_back(i);
  while (const auto project = std::dynamic_pointer_cast<const core::ProjectNode>(input)) {
    if (project->sources().size() != 1) return std::nullopt;
    const auto source = project->sources().front();
    std::vector<size_t> mapping;
    std::unordered_set<size_t> unique;
    for (const auto& expression : project->projections()) {
      const auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expression);
      if (!field || !field->inputs().empty()) return std::nullopt;
      const auto ordinal = source->outputType()->getChildIdxIfExists(field->name());
      if (!ordinal || !unique.insert(*ordinal).second ||
          !field->type()->equivalent(*source->outputType()->childAt(*ordinal))) return std::nullopt;
      mapping.push_back(*ordinal);
    }
    for (auto& ordinal : ordinals) ordinal = mapping.at(ordinal);
    result.projections.push_back(project);
    input = source;
  }
  result.node = std::dynamic_pointer_cast<const core::RowNumberNode>(input);
  if (!result.node || !result.node->generateRowNumber()) return std::nullopt;
  const auto marker = result.node->outputType()->size() - 1;
  for (size_t i = 0; i < ordinals.size(); ++i) {
    if (ordinals[i] == marker) {
      result.marker = outer->nameOf(i);
      return result;
    }
  }
  return std::nullopt;
}

// Remove only the window preparation's proven sort, never an arbitrary sort
// below projections or a caller's unrelated ordering contract.
core::PlanNodePtr topNInput(const core::RowNumberNode& rowNumber,
    const std::vector<core::FieldAccessTypedExprPtr>& sorts,
    const std::vector<core::SortOrder>& orders) {
  const auto source = rowNumber.sources().front();
  const auto sort = std::dynamic_pointer_cast<const core::OrderByNode>(source);
  const auto& partitions = rowNumber.partitionKeys();
  if (!sort || sort->sortingKeys().size() != partitions.size() + sorts.size()) return source;
  for (size_t i = 0; i < sort->sortingKeys().size(); ++i) {
    const auto partition = i < partitions.size();
    const auto& key = partition ? partitions.at(i) : sorts.at(i - partitions.size());
    const auto order = partition ? core::SortOrder(true, true) : orders.at(i - partitions.size());
    if (sort->sortingKeys().at(i)->name() != key->name() ||
        !sort->sortingKeys().at(i)->type()->equivalent(*key->type()) ||
        sort->sortingOrders().at(i) != order) return source;
  }
  return sort->sources().front();
}

}  // namespace

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertReadRel(
    const substrait::ReadRel& read,
    const std::vector<split::SplitConfig>& /*splits*/) {
  if (!read.has_base_schema() || !read.base_schema().has_struct_()) {
    throw std::runtime_error("ReadRel requires an explicit base_schema struct");
  }
  const auto logicalType = convertNamedStruct(read.base_schema());
  const auto physicalType = physicalRowType(logicalType);
  core::TypedExprPtr filter;
  if (read.has_filter()) {
    filter = convertExpression(read.filter(), physicalType);
    if (!filter || !filter->type()->isBoolean()) {
      throw std::runtime_error("ReadRel filter must have BOOLEAN type");
    }
  }
  if (read.has_best_effort_filter()) {
    const auto advisory = convertExpression(read.best_effort_filter(), physicalType);
    if (!advisory || !advisory->type()->isBoolean()) {
      throw std::runtime_error("ReadRel best_effort_filter must have BOOLEAN type");
    }
  }
  std::shared_ptr<const core::PlanNode> source;
  if (read.has_virtual_table()) {
    source = convertVirtualTable(read, physicalType);
  } else {
    source = namedTableScan(read, logicalType, logicalType, physicalType, filter);
  }
  if (filter) {
    source = std::make_shared<core::FilterNode>(nextNodeId(), filter, source);
  }
  return read.has_projection() ? convertReadProjection(read.projection(), source)
                               : source;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::namedTableScan(
    const substrait::ReadRel& read,
    const RowTypePtr& baseType,
    const RowTypePtr& scanType,
    const RowTypePtr& physicalType,
    core::TypedExprPtr filter) {
  connector::ColumnHandleMap assignments;
  for (size_t i = 0; i < scanType->size(); ++i) {
    assignments.emplace(physicalType->nameOf(i),
        std::make_shared<const PangrovaColumnHandle>(scanType->nameOf(i)));
  }
  std::string tableName = "default_table";
  if (read.has_named_table() && read.named_table().names_size() > 0) {
    tableName = read.named_table().names(read.named_table().names_size() - 1);
  }
  const auto scanId = nextNodeId();
  auto handle = std::make_shared<const PangrovaTableHandle>(
      PangrovaVeloxConnector::kConnectorId, tableName, scanId, baseType, std::move(filter),
      artifact_build_context_ ? artifact_build_context_->read_budget : nullptr);
  // External dynamic filters use logical names and these exact scan ordinals.
  scan_node_output_types_[scanId] = scanType;
  return std::make_shared<core::TableScanNode>(
      scanId, physicalType, std::move(handle), std::move(assignments));
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertFilterRel(
    const substrait::FilterRel& filter,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(filter.input(), splits);
  auto inputType = std::dynamic_pointer_cast<const RowType>(input->outputType());
  auto condition = convertExpression(filter.condition(), inputType);
  if (!condition || !condition->type()->isBoolean()) {
    throw std::runtime_error("FilterRel condition must have BOOLEAN type");
  }
  const auto projected = projectedRowNumber(input);
  if (projected) {
    const auto& rowNumber = projected->node;
    const auto sorting = row_number_sort_info_.find(rowNumber->id());
    const auto limit = extractTopNLimit(condition, projected->marker);
    if (sorting != row_number_sort_info_.end() && limit &&
        !sorting->second.sorting_keys.empty()) {
      core::PlanNodePtr result = std::make_shared<core::TopNRowNumberNode>(
          nextNodeId(), core::TopNRowNumberNode::RankFunction::kRowNumber,
          rowNumber->partitionKeys(), sorting->second.sorting_keys,
          sorting->second.sorting_orders,
          std::make_optional(rowNumber->outputType()->names().back()), *limit,
          topNInput(*rowNumber, sorting->second.sorting_keys, sorting->second.sorting_orders));
      if (result->outputType()->names() != rowNumber->outputType()->names() ||
          !result->outputType()->equivalent(*rowNumber->outputType())) {
        throw std::runtime_error("Top-N rewrite changed the declared RowNumber schema");
      }
      for (auto it = projected->projections.rbegin(); it != projected->projections.rend(); ++it) {
        result = std::make_shared<core::ProjectNode>(nextNodeId(),
            (*it)->outputType()->names(), (*it)->projections(), result);
      }
      return result;
    }
  }
  // FilterRel is the normal SQL WHERE path. Keep its authoritative FilterNode,
  // and also offer direct scan predicates to the connector's physical reader.
  // Do not push through projections, joins, limits or primary-key merge here.
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(input)) {
    if (auto handle = std::dynamic_pointer_cast<const PangrovaTableHandle>(scan->tableHandle())) {
      auto predicate = condition;
      if (handle->remainingFilter()) {
        predicate = std::make_shared<core::CallTypedExpr>(BOOLEAN(),
            std::vector<core::TypedExprPtr>{handle->remainingFilter(), condition}, "and");
      }
      auto filteredHandle = std::make_shared<const PangrovaTableHandle>(
          handle->connectorId(), handle->name(), handle->planNodeId(), handle->scanSchema(),
          predicate, handle->readBudget());
      input = std::make_shared<core::TableScanNode>(
          scan->id(), scan->outputType(), std::move(filteredHandle), scan->assignments());
    }
  }
  return std::make_shared<core::FilterNode>(nextNodeId(), condition, input);
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertProjectRel(
    const substrait::ProjectRel& project,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(project.input(), splits);
  auto inputType = std::dynamic_pointer_cast<const RowType>(input->outputType());
  for (const auto& expression : project.expressions()) {
    if (expression.has_window_function()) {
      return convertProjectWithWindowFunctions(project, input, inputType);
    }
  }
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> expressions;
  names.reserve(inputType->size() + project.expressions_size());
  expressions.reserve(names.capacity());
  for (size_t i = 0; i < inputType->size(); ++i) {
    names.push_back(physicalFieldAlias(i));
    expressions.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        inputType->childAt(i), inputType->nameOf(i)));
  }
  for (const auto& expression : project.expressions()) {
    names.push_back(physicalFieldAlias(names.size()));
    expressions.push_back(convertExpression(expression, inputType));
  }
  return std::make_shared<core::ProjectNode>(
      nextNodeId(), std::move(names), std::move(expressions), input);
}

}  // namespace pangrova::velox
