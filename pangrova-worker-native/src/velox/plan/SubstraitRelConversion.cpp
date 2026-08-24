// SubstraitRelConversion: Operator conversion methods for VeloxPlanConverter.
// Split from VeloxPlanConverter.cpp for maintainability.
//
// Contains: convertReadRel, convertFilterRel, convertProjectRel,
// convertAggregateRel, convertHashJoinRel, convertJoinRel,
// convertExchangeRel, convertSortRel, convertFetchRel, convertWindowRel,
// convertSetRel, convertCrossRel, convertSortOrder.
#include "velox/plan/VeloxPlanConverter.h"

#include <functional>
#include <limits>
#include <unordered_set>

#include "compat/NativeFeatureDetection.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/connector/PangrovaVeloxConnector.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/type/Type.h"

#include "substrait/algebra.pb.h"

#include <google/protobuf/wrappers.pb.h>
#include <sstream>

#include "pangrova/native/protocol/v1/plan_hints.pb.h"
#include "pangrova/native/protocol/v1/table_function.pb.h"
#include "velox/plan/TableFunctionNode.h"
#include "velox/runtime/NativeEngineLifecycle.h"
#include "velox/runtime/TableFunctionRegistry.h"

namespace pangrova::velox {

namespace {

// Default serialization format for shuffle exchange.
// MUST be "Presto" (capital P) — Velox's getVectorSerdeOptions() uses
// case-sensitive comparison to decide whether to create PrestoOptions
// or base VectorSerde::Options. Lowercase "presto" causes INVALID_STATE
// at runtime in PrestoSerializer::toPrestoOptions().
constexpr const char* kDefaultSerdeFormat = "Presto";

// Map Substrait HashJoinRel::JoinType to Velox JoinType.
facebook::velox::core::JoinType mapHashJoinType(
    substrait::HashJoinRel::JoinType type) {
  switch (type) {
    case substrait::HashJoinRel::JOIN_TYPE_INNER:
      return facebook::velox::core::JoinType::kInner;
    case substrait::HashJoinRel::JOIN_TYPE_LEFT:
      return facebook::velox::core::JoinType::kLeft;
    case substrait::HashJoinRel::JOIN_TYPE_RIGHT:
      return facebook::velox::core::JoinType::kRight;
    case substrait::HashJoinRel::JOIN_TYPE_OUTER:
      return facebook::velox::core::JoinType::kFull;
    case substrait::HashJoinRel::JOIN_TYPE_LEFT_SEMI:
      return facebook::velox::core::JoinType::kLeftSemiFilter;
    case substrait::HashJoinRel::JOIN_TYPE_RIGHT_SEMI:
      return facebook::velox::core::JoinType::kRightSemiFilter;
    case substrait::HashJoinRel::JOIN_TYPE_LEFT_ANTI:
      return facebook::velox::core::JoinType::kAnti;
    default:
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported HashJoinRel join type: " +
          std::to_string(static_cast<int>(type)));
  }
}

// Map Substrait JoinRel::JoinType to Velox JoinType.
facebook::velox::core::JoinType mapJoinType(
    substrait::JoinRel::JoinType type) {
  switch (type) {
    case substrait::JoinRel::JOIN_TYPE_INNER:
      return facebook::velox::core::JoinType::kInner;
    case substrait::JoinRel::JOIN_TYPE_LEFT:
      return facebook::velox::core::JoinType::kLeft;
    case substrait::JoinRel::JOIN_TYPE_RIGHT:
      return facebook::velox::core::JoinType::kRight;
    case substrait::JoinRel::JOIN_TYPE_OUTER:
      return facebook::velox::core::JoinType::kFull;
    case substrait::JoinRel::JOIN_TYPE_SEMI:
      return facebook::velox::core::JoinType::kLeftSemiFilter;
    case substrait::JoinRel::JOIN_TYPE_ANTI:
      return facebook::velox::core::JoinType::kAnti;
    default:
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported JoinRel join type: " +
          std::to_string(static_cast<int>(type)));
  }
}

// Map Substrait MergeJoinRel::JoinType to Velox JoinType.
facebook::velox::core::JoinType mapMergeJoinType(
    substrait::MergeJoinRel::JoinType type) {
  switch (type) {
    case substrait::MergeJoinRel::JOIN_TYPE_INNER:
      return facebook::velox::core::JoinType::kInner;
    case substrait::MergeJoinRel::JOIN_TYPE_LEFT:
      return facebook::velox::core::JoinType::kLeft;
    case substrait::MergeJoinRel::JOIN_TYPE_RIGHT:
      return facebook::velox::core::JoinType::kRight;
    case substrait::MergeJoinRel::JOIN_TYPE_OUTER:
      return facebook::velox::core::JoinType::kFull;
    case substrait::MergeJoinRel::JOIN_TYPE_LEFT_SEMI:
      return facebook::velox::core::JoinType::kLeftSemiFilter;
    case substrait::MergeJoinRel::JOIN_TYPE_RIGHT_SEMI:
      return facebook::velox::core::JoinType::kRightSemiFilter;
    case substrait::MergeJoinRel::JOIN_TYPE_LEFT_ANTI:
      return facebook::velox::core::JoinType::kAnti;
    default:
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported MergeJoinRel join type: " +
          std::to_string(static_cast<int>(type)));
  }
}

// Map Substrait CrossRel::JoinType to Velox JoinType.
// Defaults to kInner when unspecified (backward compat with pure cross product).
facebook::velox::core::JoinType mapCrossRelJoinType(
    substrait::CrossRel::JoinType type) {
  switch (type) {
    case substrait::CrossRel::JOIN_TYPE_INNER:
    case substrait::CrossRel::JOIN_TYPE_UNSPECIFIED:
      return facebook::velox::core::JoinType::kInner;
    case substrait::CrossRel::JOIN_TYPE_LEFT:
      return facebook::velox::core::JoinType::kLeft;
    case substrait::CrossRel::JOIN_TYPE_RIGHT:
      return facebook::velox::core::JoinType::kRight;
    case substrait::CrossRel::JOIN_TYPE_OUTER:
      return facebook::velox::core::JoinType::kFull;
    default:
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported CrossRel join type: " +
          std::to_string(static_cast<int>(type)));
  }
}

// Check if a window function has the default frame (ROWS UNBOUNDED PRECEDING
// to CURRENT ROW) — the implicit frame for row_number().
// Template to support both Expression_WindowFunction and
// ConsistentPartitionWindowRel_WindowRelFunction (same field API for bounds).
template <typename WindowFunc>
bool isDefaultRowsFrame(const WindowFunc& wf) {
  if (!wf.has_lower_bound() && !wf.has_upper_bound()) {
    return true;
  }
  bool lower_ok = !wf.has_lower_bound() ||
      wf.lower_bound().has_unbounded();
  bool upper_ok = !wf.has_upper_bound() ||
      wf.upper_bound().has_current_row();
  return lower_ok && upper_ok;
}

// ---------------------------------------------------------------------------
// TopNRowNumberNode pattern detection helpers.
//
// Detects the pattern: RowNumberNode (with ORDER BY) followed by a filter
// `row_number_col <= K` where K is a positive integer constant.
// When detected, the FilterNode + RowNumberNode are replaced with a single
// TopNRowNumberNode(limit=K) which is more efficient.
// ---------------------------------------------------------------------------

// Try to extract a constant integer value from a Velox ITypedExpr.
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

// Find a RowNumberNode in the input plan node chain.
// Handles three cases:
//   1. Input IS a RowNumberNode directly.
//   2. Input is a ProjectNode whose single source is a RowNumberNode
//      (or a GatherNode wrapping a RowNumberNode).
//   3. Input is a LocalPartitionNode (gather) wrapping a RowNumberNode.
// Returns the RowNumberNode if found, nullptr otherwise.
std::shared_ptr<const facebook::velox::core::RowNumberNode>
findRowNumberNode(
    const std::shared_ptr<const facebook::velox::core::PlanNode>& input) {
  // Case 1: input is directly a RowNumberNode.
  auto rn_node = std::dynamic_pointer_cast<
      const facebook::velox::core::RowNumberNode>(input);
  if (rn_node) {
    return rn_node;
  }

  // Case 2: input is a ProjectNode wrapping a RowNumberNode.
  auto project_node = std::dynamic_pointer_cast<
      const facebook::velox::core::ProjectNode>(input);
  if (project_node && project_node->sources().size() == 1) {
    auto proj_source = project_node->sources()[0];
    // Direct RowNumberNode under ProjectNode.
    rn_node = std::dynamic_pointer_cast<
        const facebook::velox::core::RowNumberNode>(proj_source);
    if (rn_node) {
      return rn_node;
    }
    // RowNumberNode under a LocalPartitionNode (gather) under ProjectNode.
    auto gather_node = std::dynamic_pointer_cast<
        const facebook::velox::core::LocalPartitionNode>(proj_source);
    if (gather_node && gather_node->sources().size() == 1) {
      rn_node = std::dynamic_pointer_cast<
          const facebook::velox::core::RowNumberNode>(
          gather_node->sources()[0]);
      if (rn_node) {
        return rn_node;
      }
    }
  }

  // Case 3: input is a LocalPartitionNode (gather) wrapping a RowNumberNode.
  auto gather_node = std::dynamic_pointer_cast<
      const facebook::velox::core::LocalPartitionNode>(input);
  if (gather_node && gather_node->sources().size() == 1) {
    rn_node = std::dynamic_pointer_cast<
        const facebook::velox::core::RowNumberNode>(
        gather_node->sources()[0]);
    if (rn_node) {
      return rn_node;
    }
  }

  return nullptr;
}

// Determine the row_number column name from a RowNumberNode's output type.
// RowNumberNode appends the row_number column as the last column in its output.
std::string getRowNumberColumnName(
    const std::shared_ptr<const facebook::velox::core::RowNumberNode>& rn_node) {
  auto output_type = rn_node->outputType();
  if (output_type->size() > 0) {
    return output_type->nameOf(output_type->size() - 1);
  }
  return "row_number";
}

// Detect whether a ConsistentPartitionWindowRel contains only a single
// row_number() function with no custom frame clause — eligible for
// RowNumberNode optimization.
bool isRowNumberOnlyPattern(
    const substrait::ConsistentPartitionWindowRel& window_rel,
    const std::unordered_map<uint32_t, std::string>& function_map) {
  if (window_rel.window_functions_size() != 1) {
    return false;
  }
  const auto& wf = window_rel.window_functions(0);

  std::string func_name;
  auto it = function_map.find(wf.function_reference());
  if (it != function_map.end()) {
    func_name = it->second;
  }
  if (func_name != "row_number") {
    return false;
  }
  if (wf.arguments_size() != 0) {
    return false;
  }
  if (!isDefaultRowsFrame(wf)) {
    return false;
  }
  return true;
}

// Detect whether a single Expression_WindowFunction is row_number() with
// no custom frame — eligible for RowNumberNode optimization.
bool isRowNumberOnlyWindowFunc(
    const substrait::Expression_WindowFunction& wf,
    const std::unordered_map<uint32_t, std::string>& function_map) {
  std::string func_name;
  auto it = function_map.find(wf.function_reference());
  if (it != function_map.end()) {
    func_name = it->second;
  }
  if (func_name != "row_number") {
    return false;
  }
  if (wf.arguments_size() != 0) {
    return false;
  }
  if (!isDefaultRowsFrame(wf)) {
    return false;
  }
  return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// ReadRel → TableScanNode / ValuesNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertReadRel(
    const substrait::ReadRel& read_rel,
    const std::vector<split::SplitConfig>& splits) {
  facebook::velox::RowTypePtr base_type;
  if (read_rel.has_base_schema()) {
    base_type = convertNamedStruct(read_rel.base_schema());
  } else {
    base_type = facebook::velox::ROW({"col_0"}, {facebook::velox::BIGINT()});
  }

  if (read_rel.has_virtual_table()) {
    return convertVirtualTable(read_rel, base_type);
  }

  std::string table_name = "default_table";
  if (read_rel.has_named_table() &&
      read_rel.named_table().names_size() > 0) {
    table_name = read_rel.named_table().names(
        read_rel.named_table().names_size() - 1);
  }

  // F-SUBSTRAIT-02: Apply column pruning via emit/output_mapping.
  // If the ReadRel has an emit mapping, only register column handles for the
  // projected columns rather than the full base schema. This reduces I/O
  // and memory for wide tables.
  facebook::velox::RowTypePtr output_type = base_type;
  if (read_rel.has_common() && read_rel.common().has_emit()) {
    const auto& emit = read_rel.common().emit();
    std::vector<std::string> pruned_names;
    std::vector<facebook::velox::TypePtr> pruned_types;
    pruned_names.reserve(emit.output_mapping_size());
    pruned_types.reserve(emit.output_mapping_size());
    for (int i = 0; i < emit.output_mapping_size(); ++i) {
      int idx = emit.output_mapping(i);
      if (idx >= 0 && static_cast<size_t>(idx) < base_type->size()) {
        pruned_names.push_back(base_type->nameOf(idx));
        pruned_types.push_back(base_type->childAt(idx));
      }
    }
    if (!pruned_names.empty()) {
      output_type = facebook::velox::ROW(
          std::move(pruned_names), std::move(pruned_types));
    }
  }

  facebook::velox::connector::ColumnHandleMap column_handles;
  for (size_t i = 0; i < output_type->size(); ++i) {
    auto handle = std::make_shared<const PangrovaColumnHandle>(
        output_type->nameOf(i));
    column_handles[output_type->nameOf(i)] = std::move(handle);
  }

  std::shared_ptr<const facebook::velox::core::ITypedExpr> remaining_filter;
  if (read_rel.has_filter()) {
    remaining_filter = convertExpression(read_rel.filter(), output_type);
    if (remaining_filter &&
        remaining_filter->type()->kind() !=
            facebook::velox::TypeKind::BOOLEAN) {
      remaining_filter = nullptr;
    }
  }

  facebook::velox::connector::ConnectorTableHandlePtr table_handle =
      std::make_shared<const PangrovaTableHandle>(
          PangrovaVeloxConnector::kConnectorId,
          table_name,
          remaining_filter);

  auto scan_node_id = nextNodeId();
  auto scan_node = std::make_shared<facebook::velox::core::TableScanNode>(
      scan_node_id,
      output_type,
      table_handle,
      column_handles);

  scan_node_output_types_[scan_node_id] = output_type;

  if (remaining_filter) {
    return std::make_shared<facebook::velox::core::FilterNode>(
        nextNodeId(), remaining_filter, scan_node);
  }

  return scan_node;
}

// ---------------------------------------------------------------------------
// FilterRel → FilterNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertFilterRel(
    const substrait::FilterRel& filter_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(filter_rel.input(), splits);
  auto input_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());

  // TopNRowNumberNode optimization: detect the pattern where a FilterRel
  // applies `row_number_col <= K` on top of a RowNumberNode that has
  // sorting keys (ORDER BY). Replace FilterNode + RowNumberNode with a
  // single TopNRowNumberNode(limit=K) for better performance.
  //
  // This check MUST happen BEFORE the general convertExpression() call
  // because the RowNumberNode's outputType() may not include the
  // row_number column, causing field reference resolution to fail with
  // an out-of-bounds error when the filter references the row_number
  // column by index.
  auto rn_node = findRowNumberNode(input);
  if (rn_node) {
    auto sort_it = row_number_sort_info_.find(std::string(rn_node->id()));
    if (sort_it != row_number_sort_info_.end() &&
        !sort_it->second.sorting_keys.empty()) {
      auto rn_col_name = getRowNumberColumnName(rn_node);

      // Build an extended type that includes the row_number column for
      // safe expression conversion. The RowNumberNode appends this column
      // but its outputType() may not reflect it in all configurations.
      auto extended_type = input_type;
      if (!input_type->getChildIdxIfExists(rn_col_name).has_value()) {
        std::vector<std::string> names;
        std::vector<facebook::velox::TypePtr> types;
        for (size_t i = 0; i < input_type->size(); ++i) {
          names.push_back(input_type->nameOf(i));
          types.push_back(input_type->childAt(i));
        }
        names.push_back(rn_col_name);
        types.push_back(facebook::velox::BIGINT());
        extended_type = facebook::velox::ROW(
            std::move(names), std::move(types));
      }

      // Convert filter expression using the extended type that includes
      // the row_number column, preventing out-of-bounds field resolution.
      auto topn_filter_expr = convertExpression(
          filter_rel.condition(), extended_type);
      if (topn_filter_expr) {
        auto limit = extractTopNLimit(topn_filter_expr, rn_col_name);
        if (limit.has_value()) {
          auto rn_source = rn_node->sources()[0];
          return std::make_shared<facebook::velox::core::TopNRowNumberNode>(
              nextNodeId(),
              facebook::velox::core::TopNRowNumberNode::RankFunction::kRowNumber,
              rn_node->partitionKeys(),
              sort_it->second.sorting_keys,
              sort_it->second.sorting_orders,
              /*rowNumberColumnName=*/std::make_optional(rn_col_name),
              limit.value(),
              rn_source);
        }
      }
    }
  }

  // Standard filter path: convert expression using the actual input type.
  auto filter_expr = convertExpression(filter_rel.condition(), input_type);
  if (!filter_expr ||
      filter_expr->type()->kind() != facebook::velox::TypeKind::BOOLEAN) {
    filter_expr = std::make_shared<facebook::velox::core::ConstantTypedExpr>(
        facebook::velox::BOOLEAN(), facebook::velox::variant(true));
  }

  return std::make_shared<facebook::velox::core::FilterNode>(
      nextNodeId(), filter_expr, input);
}

// ---------------------------------------------------------------------------
// ProjectRel → ProjectNode (or WindowNode + ProjectNode when window
// functions are present in the projection list)
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertProjectRel(
    const substrait::ProjectRel& project_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(project_rel.input(), splits);
  auto input_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());

  // Check if any projection expression is a window function.
  // Substrait Java SDK v0.23.0 lacks a relation-level WindowRel builder,
  // so the Java planner encodes window functions as WindowFunctionInvocation
  // expressions inside a ProjectRel.  We detect this pattern and build a
  // Velox WindowNode instead of passing them to ExprCompiler (which only
  // knows scalar functions).
  bool has_window_functions = false;
  for (int i = 0; i < project_rel.expressions_size(); ++i) {
    if (project_rel.expressions(i).has_window_function()) {
      has_window_functions = true;
      break;
    }
  }

  if (!has_window_functions) {
    // Standard path: no window functions, build a plain ProjectNode.
    std::vector<std::string> names;
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
        projections;
    names.reserve(project_rel.expressions_size());
    projections.reserve(project_rel.expressions_size());

    for (int i = 0; i < project_rel.expressions_size(); ++i) {
      auto expr = convertExpression(project_rel.expressions(i), input_type);
      projections.push_back(expr);
      auto field_access = std::dynamic_pointer_cast<
          const facebook::velox::core::FieldAccessTypedExpr>(expr);
      if (field_access && !field_access->name().empty()) {
        names.push_back(field_access->name());
      } else {
        names.push_back("proj_" + std::to_string(i));
      }
    }

    return std::make_shared<facebook::velox::core::ProjectNode>(
        nextNodeId(), std::move(names), std::move(projections), input);
  }

  // Window function path: extract window functions from the projection,
  // build a WindowNode, then build a final ProjectNode that maps the
  // output columns to the expected order.
  return convertProjectWithWindowFunctions(project_rel, input, input_type);
}

// ---------------------------------------------------------------------------
// AggregateRel helpers
// ---------------------------------------------------------------------------

std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
VeloxPlanConverter::convertGroupingKeys(
    const substrait::AggregateRel& aggregate_rel,
    const facebook::velox::RowTypePtr& input_type) {
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      grouping_keys;
  if (aggregate_rel.groupings_size() == 0) {
    return grouping_keys;
  }
  const auto& grouping = aggregate_rel.groupings(0);
  for (const auto& expr : grouping.grouping_expressions()) {
    auto converted = convertExpression(expr, input_type);
    auto field_access = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(converted);
    if (field_access) {
      grouping_keys.push_back(field_access);
    }
  }
  return grouping_keys;
}

void VeloxPlanConverter::convertAggregateMeasures(
    const substrait::AggregateRel& aggregate_rel,
    const facebook::velox::RowTypePtr& input_type,
    const std::unordered_set<std::string>& grouping_key_names,
    std::vector<std::string>& aggregate_names,
    std::vector<facebook::velox::core::AggregationNode::Aggregate>& aggregates) {
  for (int i = 0; i < aggregate_rel.measures_size(); ++i) {
    const auto& measure = aggregate_rel.measures(i);
    std::string name = "agg_" + std::to_string(i);
    while (grouping_key_names.count(name) > 0) {
      name += "_m";
    }
    aggregate_names.push_back(name);

    facebook::velox::core::AggregationNode::Aggregate agg;
    if (measure.has_measure()) {
      const auto& agg_func = measure.measure();
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
          args;
      for (const auto& arg : agg_func.arguments()) {
        if (arg.has_value()) {
          args.push_back(convertExpression(arg.value(), input_type));
        }
      }
      if (args.empty() && agg_func.args_size() > 0) {
        for (const auto& expr : agg_func.args()) {
          args.push_back(convertExpression(expr, input_type));
        }
      }
      auto output_type = convertType(agg_func.output_type());
      std::string func_name;
      auto it = function_map_.find(agg_func.function_reference());
      if (it != function_map_.end()) {
        func_name = it->second;
      } else {
        func_name =
            "agg_func_" + std::to_string(agg_func.function_reference());
      }
      agg.call = std::make_shared<facebook::velox::core::CallTypedExpr>(
          output_type, std::move(args), func_name);
      for (const auto& input : agg.call->inputs()) {
        agg.rawInputTypes.push_back(input->type());
      }

      // SQL:2003 aggregate FILTER (WHERE ...) → Velox per-aggregate mask.
      // The planner pushes the condition down as a boolean input column, so
      // the filter expression here must be a boolean field reference. Anything
      // else indicates a planner contract violation — fail loud, never drop
      // the filter silently (that would return wrong aggregate results).
      if (measure.has_filter()) {
        auto phase = agg_func.phase();
        // Mask is only valid where raw input rows are visible: single-step
        // or the partial step of a split aggregation. Applying it during
        // intermediate/final steps would re-filter partial accumulators.
        if (phase == 2 || phase == 4) {
          throw std::runtime_error(
              "AggregateRel measure " + std::to_string(i) +
              " has a FILTER mask on intermediate/final aggregation phase " +
              std::to_string(phase) +
              "; masks are only valid for single or partial phases");
        }
        auto mask_expr = convertExpression(measure.filter(), input_type);
        auto mask_field = std::dynamic_pointer_cast<
            const facebook::velox::core::FieldAccessTypedExpr>(mask_expr);
        if (!mask_field ||
            mask_field->type()->kind() !=
                facebook::velox::TypeKind::BOOLEAN) {
          throw std::runtime_error(
              "AggregateRel measure " + std::to_string(i) +
              " FILTER expression must be a boolean field reference, got: " +
              (mask_expr ? mask_expr->type()->toString() : "null"));
        }
        agg.mask = mask_field;
      }
    }
    aggregates.push_back(std::move(agg));
  }
}

facebook::velox::core::AggregationNode::Step
VeloxPlanConverter::resolveAggregationStep(
    const substrait::AggregateRel& aggregate_rel) {
  if (aggregate_rel.measures_size() == 0 ||
      !aggregate_rel.measures(0).has_measure()) {
    return facebook::velox::core::AggregationNode::Step::kSingle;
  }
  auto phase = aggregate_rel.measures(0).measure().phase();
  switch (phase) {
    case 1: return facebook::velox::core::AggregationNode::Step::kPartial;
    case 2: return facebook::velox::core::AggregationNode::Step::kIntermediate;
    case 3: return facebook::velox::core::AggregationNode::Step::kSingle;
    case 4: return facebook::velox::core::AggregationNode::Step::kFinal;
    default: return facebook::velox::core::AggregationNode::Step::kSingle;
  }
}

std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
VeloxPlanConverter::detectPreGroupedKeys(
    const substrait::AggregateRel& aggregate_rel,
    const facebook::velox::RowTypePtr& input_type,
    const std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& grouping_keys) {
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      pre_grouped_keys;
  if (grouping_keys.empty()) {
    return pre_grouped_keys;
  }

  // L2-5: Determine the sort fields that guarantee input ordering.
  // Case 1: Direct SortRel input (e.g., Aggregate(Sort(input))).
  // Case 2: ExchangeRel whose sub-input is a SortRel
  //         (e.g., Aggregate(MergeExchange(Sort(input)))). A MergeExchange
  //         preserves the sort order from the sender, so data arriving at
  //         the aggregate is sorted by those fields.
  const substrait::SortRel* sort_rel_ptr = nullptr;
  if (aggregate_rel.input().has_sort()) {
    sort_rel_ptr = &aggregate_rel.input().sort();
  } else if (aggregate_rel.input().has_exchange() &&
             aggregate_rel.input().exchange().input().has_sort()) {
    sort_rel_ptr = &aggregate_rel.input().exchange().input().sort();
  }

  if (!sort_rel_ptr) {
    return pre_grouped_keys;
  }
  const auto& sort_rel = *sort_rel_ptr;

  size_t match_count = 0;
  for (int s = 0; s < sort_rel.sorts_size() &&
       match_count < grouping_keys.size(); ++s) {
    const auto& sort_field = sort_rel.sorts(s);
    if (!sort_field.has_expr()) break;
    auto sort_expr = convertExpression(sort_field.expr(), input_type);
    auto sort_field_access = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(sort_expr);
    if (sort_field_access &&
        sort_field_access->name() == grouping_keys[match_count]->name()) {
      ++match_count;
    } else {
      break;
    }
  }
  if (match_count == grouping_keys.size()) {
    pre_grouped_keys = grouping_keys;
  }
  return pre_grouped_keys;
}

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
    if (idx.has_value()) {
      key_channels.push_back(
          static_cast<facebook::velox::column_index_t>(idx.value()));
    }
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
    const std::vector<std::string>& aggregate_names,
    const std::vector<facebook::velox::core::AggregationNode::Aggregate>& aggregates,
    std::shared_ptr<const facebook::velox::core::PlanNode> input) {
  // Build partial aggregates with INTERMEDIATE output types.
  // Velox's extractAccumulators casts to RowVector — wrong type causes SIGSEGV.
  std::vector<facebook::velox::core::AggregationNode::Aggregate> partialAggs;
  partialAggs.reserve(aggregates.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    auto pa = aggregates[i];
    pa.call = std::make_shared<facebook::velox::core::CallTypedExpr>(
        facebook::velox::exec::resolveIntermediateType(
            aggregates[i].call->name(), aggregates[i].rawInputTypes),
        aggregates[i].call->inputs(), aggregates[i].call->name());
    partialAggs.push_back(std::move(pa));
  }
  auto partialNode = std::make_shared<facebook::velox::core::AggregationNode>(
      nextNodeId(), facebook::velox::core::AggregationNode::Step::kPartial,
      grouping_keys, pre_grouped_keys, aggregate_names, partialAggs,
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
      finalGK, noPreGrouped, aggregate_names, finalAggs, false, false, exchange);
}

// ---------------------------------------------------------------------------
// AggregateRel → AggregationNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertAggregateRel(
    const substrait::AggregateRel& aggregate_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(aggregate_rel.input(), splits);
  auto input_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());

  // Multi-grouping-set expansion: GROUPING SETS / CUBE / ROLLUP.
  // Uses GroupIdNode to replicate rows once per grouping set, then a single
  // AggregationNode groups by all keys + groupId. Reads input only once.
  // Version gate: compile-time static_assert ensures GroupIdNode availability.
  if (aggregate_rel.groupings_size() > 1) {
    return convertMultiGroupingAggregate(aggregate_rel, input, input_type);
  }

  // Detect multi-distinct pattern: ≥2 DISTINCT measures on different columns.
  // When detected, insert MarkDistinctNode chain before the AggregationNode.
  // Single-DISTINCT uses native AggregationNode distinct flag instead.
  auto multi_distinct = detectMultiDistinctColumns(aggregate_rel, input_type);
  if (multi_distinct.is_multi_distinct) {
    return buildMarkDistinctAggregation(
        aggregate_rel, multi_distinct, input, input_type);
  }

  auto grouping_keys = convertGroupingKeys(aggregate_rel, input_type);

  std::unordered_set<std::string> grouping_key_names;
  for (const auto& gk : grouping_keys) {
    grouping_key_names.insert(gk->name());
  }

  std::vector<std::string> aggregate_names;
  std::vector<facebook::velox::core::AggregationNode::Aggregate> aggregates;
  convertAggregateMeasures(
      aggregate_rel, input_type, grouping_key_names,
      aggregate_names, aggregates);

  // Set distinct flag on aggregates that have DISTINCT invocation.
  // For single-DISTINCT, the native AggregationNode handles it directly.
  bool has_distinct = false;
  for (int i = 0; i < aggregate_rel.measures_size(); ++i) {
    const auto& measure = aggregate_rel.measures(i);
    if (measure.has_measure() &&
        measure.measure().invocation() ==
            substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT) {
      aggregates[i].distinct = true;
      has_distinct = true;
    }
  }

  auto step = resolveAggregationStep(aggregate_rel);
  auto pre_grouped_keys =
      detectPreGroupedKeys(aggregate_rel, input_type, grouping_keys);

  // Parallel-safe aggregation: split kSingle into kPartial→kFinal.
  // Exception: DISTINCT aggregates cannot be split into partial/final
  // because partial distinct sets from different pipelines are not mergeable.
  // Instead, repartition by grouping keys and run kSingle with distinct=true.
  if (step == facebook::velox::core::AggregationNode::Step::kSingle) {
    if (has_distinct) {
      auto colocated = insertLocalPartitionExchange(input, grouping_keys);
      return std::make_shared<facebook::velox::core::AggregationNode>(
          nextNodeId(),
          facebook::velox::core::AggregationNode::Step::kSingle,
          grouping_keys, pre_grouped_keys,
          aggregate_names, aggregates,
          /*ignoreNullKeys=*/false,
          /*noGroupsSpanBatches=*/false,
          colocated);
    }
    return buildParallelAggregation(
        grouping_keys, pre_grouped_keys,
        aggregate_names, aggregates, input);
  }

  return std::make_shared<facebook::velox::core::AggregationNode>(
      nextNodeId(), step, grouping_keys,
      /*preGroupedKeys=*/pre_grouped_keys,
      aggregate_names, aggregates,
      /*ignoreNullKeys=*/false,
      /*noGroupsSpanBatches=*/false,
      input);
}

// ---------------------------------------------------------------------------
// Multi-distinct detection: identifies ≥2 DISTINCT measures on different columns.
// Single-DISTINCT does NOT trigger multi-distinct path — the native
// AggregationNode handles it via the distinct flag on Aggregate.
// ---------------------------------------------------------------------------

VeloxPlanConverter::MultiDistinctDetection
VeloxPlanConverter::detectMultiDistinctColumns(
    const substrait::AggregateRel& aggregate_rel,
    const facebook::velox::RowTypePtr& input_type) {
  MultiDistinctDetection result;

  // Collect distinct columns: map column_index → DistinctColumnInfo.
  std::unordered_map<int, size_t> col_index_to_info_index;

  for (int i = 0; i < aggregate_rel.measures_size(); ++i) {
    const auto& measure = aggregate_rel.measures(i);
    if (!measure.has_measure()) {
      continue;
    }
    const auto& agg_func = measure.measure();
    if (agg_func.invocation() !=
        substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT) {
      continue;
    }

    // Extract the first argument as the distinct column.
    // DISTINCT aggregates operate on a single column (e.g., COUNT(DISTINCT a)).
    int col_index = -1;
    if (agg_func.arguments_size() > 0 && agg_func.arguments(0).has_value()) {
      const auto& arg_expr = agg_func.arguments(0).value();
      if (arg_expr.has_selection() &&
          arg_expr.selection().has_direct_reference() &&
          arg_expr.selection().direct_reference().has_struct_field()) {
        col_index = arg_expr.selection().direct_reference().struct_field().field();
      }
    } else if (agg_func.args_size() > 0) {
      // Deprecated args field (Substrait proto backward compat).
      const auto& arg_expr = agg_func.args(0);
      if (arg_expr.has_selection() &&
          arg_expr.selection().has_direct_reference() &&
          arg_expr.selection().direct_reference().has_struct_field()) {
        col_index = arg_expr.selection().direct_reference().struct_field().field();
      }
    }

    if (col_index < 0 ||
        col_index >= static_cast<int>(input_type->size())) {
      continue;
    }

    auto it = col_index_to_info_index.find(col_index);
    if (it != col_index_to_info_index.end()) {
      // Same column already seen — add this measure to existing entry.
      result.distinct_columns[it->second].measure_indices.push_back(i);
    } else {
      // New distinct column.
      DistinctColumnInfo info;
      info.measure_indices.push_back(i);
      info.column_name = input_type->nameOf(col_index);
      info.column_index = col_index;
      col_index_to_info_index[col_index] = result.distinct_columns.size();
      result.distinct_columns.push_back(std::move(info));
    }
  }

  // Multi-distinct: ≥2 DISTINCT measures on different columns.
  result.is_multi_distinct = result.distinct_columns.size() >= 2;
  return result;
}

// ---------------------------------------------------------------------------
// Multi-distinct aggregation via MarkDistinctNode chain.
//
// Strategy: For each distinct column, insert a MarkDistinctNode that marks
// the first occurrence of each distinct value within each group with a boolean
// marker column. The final AggregationNode uses these markers as masks:
//   COUNT(DISTINCT a) → COUNT(a) WHERE mark_distinct_0 = true
//   COUNT(DISTINCT b) → COUNT(b) WHERE mark_distinct_1 = true
//
// Chain: input → MarkDistinct(col_a) → MarkDistinct(col_b) → AggregationNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::buildMarkDistinctAggregation(
    const substrait::AggregateRel& aggregate_rel,
    const MultiDistinctDetection& multi_distinct,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const facebook::velox::RowTypePtr& input_type) {
  auto grouping_keys = convertGroupingKeys(aggregate_rel, input_type);

  // Build MarkDistinctNode chain: one per distinct column.
  // Each MarkDistinctNode groups by (grouping_keys + distinct_column) and
  // produces a boolean marker column for the first occurrence.
  auto current = input;
  std::vector<std::string> marker_names;

  for (size_t i = 0; i < multi_distinct.distinct_columns.size(); ++i) {
    const auto& dc = multi_distinct.distinct_columns[i];
    std::string marker_name = "mark_distinct_" + std::to_string(i);
    marker_names.push_back(marker_name);

    // distinctKeys = grouping_keys + the distinct column itself.
    std::vector<facebook::velox::core::FieldAccessTypedExprPtr> distinct_keys;
    auto current_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
        current->outputType());

    for (const auto& gk : grouping_keys) {
      auto idx = current_type->getChildIdxIfExists(gk->name());
      if (idx.has_value()) {
        distinct_keys.push_back(
            std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
                current_type->childAt(idx.value()), gk->name()));
      }
    }

    // Add the distinct column itself.
    auto col_idx = current_type->getChildIdxIfExists(dc.column_name);
    if (col_idx.has_value()) {
      distinct_keys.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              current_type->childAt(col_idx.value()), dc.column_name));
    }

    current = std::make_shared<facebook::velox::core::MarkDistinctNode>(
        nextNodeId(), marker_name, std::move(distinct_keys), current);
  }

  // Build the final AggregationNode with marker-masked aggregates.
  auto mark_output_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      current->outputType());

  // Rebuild grouping keys referencing the MarkDistinctNode output.
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      agg_grouping_keys;
  std::unordered_set<std::string> grouping_key_names;
  for (const auto& gk : grouping_keys) {
    auto idx = mark_output_type->getChildIdx(gk->name());
    agg_grouping_keys.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            mark_output_type->childAt(idx), gk->name()));
    grouping_key_names.insert(gk->name());
  }

  // Build aggregate measures with masks applied to DISTINCT aggregates.
  std::vector<std::string> aggregate_names;
  std::vector<facebook::velox::core::AggregationNode::Aggregate> aggregates;

  // Map: column_index → marker index (for looking up which marker to use).
  std::unordered_map<int, size_t> col_to_marker;
  for (size_t i = 0; i < multi_distinct.distinct_columns.size(); ++i) {
    col_to_marker[multi_distinct.distinct_columns[i].column_index] = i;
  }

  for (int i = 0; i < aggregate_rel.measures_size(); ++i) {
    const auto& measure = aggregate_rel.measures(i);
    std::string name = "agg_" + std::to_string(i);
    while (grouping_key_names.count(name) > 0) {
      name += "_m";
    }
    aggregate_names.push_back(name);

    facebook::velox::core::AggregationNode::Aggregate agg;
    if (measure.has_measure()) {
      const auto& agg_func = measure.measure();
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> args;
      for (const auto& arg : agg_func.arguments()) {
        if (arg.has_value()) {
          args.push_back(convertExpression(arg.value(), mark_output_type));
        }
      }
      if (args.empty() && agg_func.args_size() > 0) {
        for (const auto& expr : agg_func.args()) {
          args.push_back(convertExpression(expr, mark_output_type));
        }
      }
      auto output_type = convertType(agg_func.output_type());
      std::string func_name;
      auto it = function_map_.find(agg_func.function_reference());
      if (it != function_map_.end()) {
        func_name = it->second;
      } else {
        func_name = "agg_func_" + std::to_string(agg_func.function_reference());
      }
      agg.call = std::make_shared<facebook::velox::core::CallTypedExpr>(
          output_type, std::move(args), func_name);
      for (const auto& inp : agg.call->inputs()) {
        agg.rawInputTypes.push_back(inp->type());
      }

      // For DISTINCT aggregates, set the marker column as mask.
      if (agg_func.invocation() ==
          substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT) {
        // Defensive: the planner rejects FILTER on DISTINCT measures in
        // multi-distinct aggregations (the marker mask slot is already
        // taken). Reaching here means the Java-side guard was bypassed.
        if (measure.has_filter()) {
          throw std::runtime_error(
              "AggregateRel measure " + std::to_string(i) +
              " combines DISTINCT and FILTER in a multi-distinct "
              "aggregation; the planner must reject this combination");
        }
        int col_index = -1;
        if (agg_func.arguments_size() > 0 && agg_func.arguments(0).has_value()) {
          const auto& arg_expr = agg_func.arguments(0).value();
          if (arg_expr.has_selection() &&
              arg_expr.selection().has_direct_reference() &&
              arg_expr.selection().direct_reference().has_struct_field()) {
            col_index = arg_expr.selection().direct_reference().struct_field().field();
          }
        } else if (agg_func.args_size() > 0) {
          const auto& arg_expr = agg_func.args(0);
          if (arg_expr.has_selection() &&
              arg_expr.selection().has_direct_reference() &&
              arg_expr.selection().direct_reference().has_struct_field()) {
            col_index = arg_expr.selection().direct_reference().struct_field().field();
          }
        }

        auto marker_it = col_to_marker.find(col_index);
        if (marker_it != col_to_marker.end()) {
          const auto& marker_name = marker_names[marker_it->second];
          auto marker_idx = mark_output_type->getChildIdx(marker_name);
          agg.mask = std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              mark_output_type->childAt(marker_idx), marker_name);
        }
      } else if (measure.has_filter()) {
        // Non-DISTINCT measure with a SQL:2003 FILTER clause: the filter
        // column passes through the MarkDistinctNode chain unchanged (markers
        // are appended after the input columns), so the same mask mechanism
        // applies. Same contract as the single-path conversion: boolean
        // field reference only, fail loud otherwise.
        auto mask_expr = convertExpression(measure.filter(), mark_output_type);
        auto mask_field = std::dynamic_pointer_cast<
            const facebook::velox::core::FieldAccessTypedExpr>(mask_expr);
        if (!mask_field ||
            mask_field->type()->kind() !=
                facebook::velox::TypeKind::BOOLEAN) {
          throw std::runtime_error(
              "AggregateRel measure " + std::to_string(i) +
              " FILTER expression must be a boolean field reference, got: " +
              (mask_expr ? mask_expr->type()->toString() : "null"));
        }
        agg.mask = mask_field;
      }
    }
    aggregates.push_back(std::move(agg));
  }

  std::vector<facebook::velox::core::FieldAccessTypedExprPtr> no_pre_grouped;
  return std::make_shared<facebook::velox::core::AggregationNode>(
      nextNodeId(),
      facebook::velox::core::AggregationNode::Step::kSingle,
      agg_grouping_keys, no_pre_grouped,
      aggregate_names, aggregates,
      /*ignoreNullKeys=*/false,
      /*noGroupsSpanBatches=*/false,
      current);
}

// ---------------------------------------------------------------------------
// Multi-grouping-set expansion: GROUPING SETS / CUBE / ROLLUP
//
// Strategy: GroupIdNode + single AggregationNode + ProjectNode.
//   GroupIdNode replicates each input row N times (one per grouping set),
//   NULLing columns not in that set, and appends a groupId column.
//   A single AggregationNode groups by all keys + groupId.
//   A final ProjectNode strips groupId from the output.
//
// This reads input only once (vs. the old N-branch approach that duplicated
// input reads). Velox optimizes GroupIdNode internally with lazy NULL
// materialization.
//
// Version gate: GroupIdNode requires velox/core/PlanNode.h (verified via
// PANGROVA_HAS_VELOX_GROUP_ID_NODE). Build fails if unavailable.
// ---------------------------------------------------------------------------

static_assert(
    PANGROVA_HAS_VELOX_GROUP_ID_NODE,
    "GroupIdNode is required for GROUPING SETS support. "
    "Upgrade Velox to a version that includes velox/core/PlanNode.h with "
    "GroupIdNode (any version >= 0.1 supported by Pangrova).");

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertMultiGroupingAggregate(
    const substrait::AggregateRel& aggregate_rel,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const facebook::velox::RowTypePtr& input_type) {
  // Collect the union of all grouping key column names across all sets.
  // Preserves insertion order for deterministic output schema.
  std::vector<std::string> all_key_names;
  std::unordered_set<std::string> all_key_set;
  for (int g = 0; g < aggregate_rel.groupings_size(); ++g) {
    const auto& grouping = aggregate_rel.groupings(g);
    for (const auto& expr : grouping.grouping_expressions()) {
      auto converted = convertExpression(expr, input_type);
      auto fa = std::dynamic_pointer_cast<
          const facebook::velox::core::FieldAccessTypedExpr>(converted);
      if (fa && all_key_set.find(fa->name()) == all_key_set.end()) {
        all_key_names.push_back(fa->name());
        all_key_set.insert(fa->name());
      }
    }
  }

  // Build measures (shared across all grouping sets).
  std::unordered_set<std::string> key_name_set(
      all_key_names.begin(), all_key_names.end());
  std::vector<std::string> agg_names;
  std::vector<facebook::velox::core::AggregationNode::Aggregate> aggregates;
  convertAggregateMeasures(
      aggregate_rel, input_type, key_name_set, agg_names, aggregates);

  // --- Build GroupIdNode ---
  // GroupIdNode output schema: [grouping_keys..., aggregation_inputs..., groupId]
  // groupingSets: for each set, list the output column names that are active.
  // groupingKeyInfos: maps output column name to input field.
  // aggregationInputs: columns consumed by aggregate functions (pass-through).

  // Build groupingKeyInfos: one entry per unique grouping key.
  std::vector<facebook::velox::core::GroupIdNode::GroupingKeyInfo>
      grouping_key_infos;
  for (const auto& key_name : all_key_names) {
    facebook::velox::core::GroupIdNode::GroupingKeyInfo info;
    info.output = key_name;
    auto idx = input_type->getChildIdxIfExists(key_name);
    info.input = std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
        idx.has_value() ? input_type->childAt(idx.value())
                        : facebook::velox::BIGINT(),
        key_name);
    grouping_key_infos.push_back(std::move(info));
  }

  // Build groupingSets: each set contains the output column names active in it.
  std::vector<std::vector<std::string>> grouping_sets;
  for (int g = 0; g < aggregate_rel.groupings_size(); ++g) {
    const auto& grouping = aggregate_rel.groupings(g);
    std::vector<std::string> set_keys;
    for (const auto& expr : grouping.grouping_expressions()) {
      auto converted = convertExpression(expr, input_type);
      auto fa = std::dynamic_pointer_cast<
          const facebook::velox::core::FieldAccessTypedExpr>(converted);
      if (fa) {
        set_keys.push_back(fa->name());
      }
    }
    grouping_sets.push_back(std::move(set_keys));
  }

  // Build aggregationInputs: columns referenced by aggregate measures.
  // These are passed through GroupIdNode unchanged.
  std::vector<facebook::velox::core::FieldAccessTypedExprPtr> aggregation_inputs;
  std::unordered_set<std::string> agg_input_names;
  for (int i = 0; i < aggregate_rel.measures_size(); ++i) {
    const auto& measure = aggregate_rel.measures(i);
    if (measure.has_measure()) {
      const auto& agg_func = measure.measure();
      for (const auto& arg : agg_func.arguments()) {
        if (arg.has_value()) {
          auto expr = convertExpression(arg.value(), input_type);
          auto fa = std::dynamic_pointer_cast<
              const facebook::velox::core::FieldAccessTypedExpr>(expr);
          if (fa &&
              agg_input_names.find(fa->name()) == agg_input_names.end()) {
            aggregation_inputs.push_back(fa);
            agg_input_names.insert(fa->name());
          }
        }
      }
      if (agg_func.arguments_size() == 0) {
        for (const auto& expr : agg_func.args()) {
          auto converted = convertExpression(expr, input_type);
          auto fa = std::dynamic_pointer_cast<
              const facebook::velox::core::FieldAccessTypedExpr>(converted);
          if (fa &&
              agg_input_names.find(fa->name()) == agg_input_names.end()) {
            aggregation_inputs.push_back(fa);
            agg_input_names.insert(fa->name());
          }
        }
      }
    }
  }

  const std::string group_id_name = "groupId";

  auto group_id_node = std::make_shared<facebook::velox::core::GroupIdNode>(
      nextNodeId(),
      std::move(grouping_sets),
      std::move(grouping_key_infos),
      std::move(aggregation_inputs),
      group_id_name,
      input);

  // --- Build AggregationNode ---
  // Group by: all grouping keys + groupId.
  auto gid_output_type =
      std::dynamic_pointer_cast<const facebook::velox::RowType>(
          group_id_node->outputType());

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      agg_grouping_keys;
  for (const auto& key_name : all_key_names) {
    auto idx = gid_output_type->getChildIdx(key_name);
    agg_grouping_keys.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            gid_output_type->childAt(idx), key_name));
  }
  // Add groupId as a grouping key.
  auto gid_idx = gid_output_type->getChildIdx(group_id_name);
  agg_grouping_keys.push_back(
      std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
          gid_output_type->childAt(gid_idx), group_id_name));

  // Rebuild aggregate measures referencing GroupIdNode output columns.
  std::vector<std::string> gid_agg_names;
  std::vector<facebook::velox::core::AggregationNode::Aggregate> gid_aggregates;
  for (int i = 0; i < aggregate_rel.measures_size(); ++i) {
    const auto& measure = aggregate_rel.measures(i);
    std::string name = "agg_" + std::to_string(i);
    while (key_name_set.count(name) > 0) {
      name += "_m";
    }
    gid_agg_names.push_back(name);

    facebook::velox::core::AggregationNode::Aggregate agg;
    if (measure.has_measure()) {
      const auto& agg_func = measure.measure();
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
          args;
      for (const auto& arg : agg_func.arguments()) {
        if (arg.has_value()) {
          args.push_back(convertExpression(arg.value(), gid_output_type));
        }
      }
      if (args.empty() && agg_func.args_size() > 0) {
        for (const auto& expr : agg_func.args()) {
          args.push_back(convertExpression(expr, gid_output_type));
        }
      }
      auto output_type = convertType(agg_func.output_type());
      std::string func_name;
      auto it = function_map_.find(agg_func.function_reference());
      if (it != function_map_.end()) {
        func_name = it->second;
      } else {
        func_name =
            "agg_func_" + std::to_string(agg_func.function_reference());
      }
      agg.call = std::make_shared<facebook::velox::core::CallTypedExpr>(
          output_type, std::move(args), func_name);
      for (const auto& inp : agg.call->inputs()) {
        agg.rawInputTypes.push_back(inp->type());
      }
    }
    gid_aggregates.push_back(std::move(agg));
  }

  std::vector<facebook::velox::core::FieldAccessTypedExprPtr> no_pre_grouped;
  auto agg_node = std::make_shared<facebook::velox::core::AggregationNode>(
      nextNodeId(),
      facebook::velox::core::AggregationNode::Step::kSingle,
      agg_grouping_keys, no_pre_grouped,
      gid_agg_names, gid_aggregates,
      /*ignoreNullKeys=*/false,
      /*noGroupsSpanBatches=*/false,
      group_id_node);

  // --- Build final ProjectNode to strip groupId ---
  // Output schema: [all_keys..., aggregates...] (no groupId).
  auto agg_output_type =
      std::dynamic_pointer_cast<const facebook::velox::RowType>(
          agg_node->outputType());

  std::vector<std::string> final_names;
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
      final_exprs;

  // Pass through grouping key columns.
  for (const auto& key_name : all_key_names) {
    final_names.push_back(key_name);
    auto idx = agg_output_type->getChildIdx(key_name);
    final_exprs.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            agg_output_type->childAt(idx), key_name));
  }

  // Pass through aggregate result columns.
  for (const auto& agg_name : gid_agg_names) {
    final_names.push_back(agg_name);
    auto idx = agg_output_type->getChildIdx(agg_name);
    final_exprs.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            agg_output_type->childAt(idx), agg_name));
  }

  return std::make_shared<facebook::velox::core::ProjectNode>(
      nextNodeId(), std::move(final_names), std::move(final_exprs), agg_node);
}

// ---------------------------------------------------------------------------
// ExtensionSingleRel → UnnestNode or VectorSearch (Project+TopN)
//
// Dispatches on the "op" field in the JSON detail payload:
//   "vector_search" → Project(distance function) + TopN
//   "unnest"        → UnnestNode + remap ProjectNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertExtensionSingleRel(
    const substrait::ExtensionSingleRel& ext_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(ext_rel.input(), splits);
  auto input_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());

  // Parse the detail payload (packed as google.protobuf.StringValue in Any).
  std::string detail_json;
  if (ext_rel.has_detail()) {
    google::protobuf::StringValue sv;
    if (ext_rel.detail().UnpackTo(&sv)) {
      detail_json = sv.value();
    }
  }

  // Dispatch on op field.
  if (detail_json.find("\"op\":\"vector_search\"") != std::string::npos) {
    return convertVectorSearchExtension(detail_json, input, input_type);
  }

  // Simple JSON parsing for the unnest metadata.
  // Expected: {"op":"unnest","unnestColumns":[0],"unnestNames":["val"],"ordinality":false}
  std::vector<int> unnest_col_indices;
  std::vector<std::string> unnest_names;
  bool with_ordinality = false;

  // Parse unnestColumns array.
  auto cols_start = detail_json.find("\"unnestColumns\":[");
  if (cols_start != std::string::npos) {
    auto arr_start = detail_json.find('[', cols_start) + 1;
    auto arr_end = detail_json.find(']', arr_start);
    std::string arr = detail_json.substr(arr_start, arr_end - arr_start);
    std::istringstream ss(arr);
    std::string token;
    while (std::getline(ss, token, ',')) {
      if (!token.empty()) {
        unnest_col_indices.push_back(std::stoi(token));
      }
    }
  }

  // Parse unnestNames array.
  auto names_start = detail_json.find("\"unnestNames\":[");
  if (names_start != std::string::npos) {
    auto arr_start = detail_json.find('[', names_start) + 1;
    auto arr_end = detail_json.find(']', arr_start);
    std::string arr = detail_json.substr(arr_start, arr_end - arr_start);
    size_t pos = 0;
    while ((pos = arr.find('"', pos)) != std::string::npos) {
      auto end = arr.find('"', pos + 1);
      if (end != std::string::npos) {
        unnest_names.push_back(arr.substr(pos + 1, end - pos - 1));
        pos = end + 1;
      } else {
        break;
      }
    }
  }

  // Parse ordinality flag.
  with_ordinality = detail_json.find("\"ordinality\":true") != std::string::npos;

  // Build replicateVariables: all input columns except the unnest columns.
  std::unordered_set<int> unnest_set(
      unnest_col_indices.begin(), unnest_col_indices.end());
  std::vector<facebook::velox::core::FieldAccessTypedExprPtr> replicate_vars;
  for (size_t i = 0; i < input_type->size(); ++i) {
    if (unnest_set.count(static_cast<int>(i)) == 0) {
      replicate_vars.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              input_type->childAt(i), input_type->nameOf(i)));
    }
  }

  // Build unnestVariables: the ARRAY/MAP columns to expand.
  std::vector<facebook::velox::core::FieldAccessTypedExprPtr> unnest_vars;
  for (int idx : unnest_col_indices) {
    auto col_type = input_type->childAt(idx);
    LOG(INFO) << "event=unnest_var idx=" << idx
              << " name=" << input_type->nameOf(idx)
              << " type=" << col_type->toString();
    unnest_vars.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            col_type, input_type->nameOf(idx)));
  }

  LOG(INFO) << "event=unnest_config input_type=" << input_type->toString()
            << " unnest_col_indices.size=" << unnest_col_indices.size()
            << " unnest_names.size=" << unnest_names.size()
            << " with_ordinality=" << with_ordinality;

  // Ordinality column name (for POSEXPLODE).
  std::optional<std::string> ordinality_name;
  if (with_ordinality) {
    ordinality_name = "ordinality";
  }

  auto unnest_node = std::make_shared<facebook::velox::core::UnnestNode>(
      nextNodeId(),
      std::move(replicate_vars),
      std::move(unnest_vars),
      std::move(unnest_names),
      std::move(ordinality_name),
      std::nullopt,  // no marker column
      input);

  // Wrap UnnestNode in a ProjectNode to reconstruct the correlate's column
  // layout. The Java planner uses correlate.getRowType() as the output type:
  //   (left_col_0, ..., left_col_N, unnest_output_0, ...)
  // But the Velox UnnestNode output excludes unnest source columns:
  //   (replicate_cols..., unnest_output_cols...)
  // The parent ProjectRel uses field indices based on the correlate layout,
  // so we insert a mapping ProjectNode that places NULL constants at the
  // unnest source column positions to align indices correctly.
  auto unnest_output_type =
      std::dynamic_pointer_cast<const facebook::velox::RowType>(
          unnest_node->outputType());

  // Build the remapped projection: for each input column position,
  // emit either a field reference (for non-unnest cols) or a typed NULL
  // (for unnest source cols). Then append unnest output columns.
  std::vector<std::string> proj_names;
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
      proj_exprs;

  // Track position in unnest_node output for replicate columns.
  int replicate_idx = 0;
  for (size_t i = 0; i < input_type->size(); ++i) {
    if (unnest_set.count(static_cast<int>(i)) == 0) {
      // Replicate column: reference from UnnestNode output.
      proj_names.push_back(input_type->nameOf(i));
      proj_exprs.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              unnest_output_type->childAt(replicate_idx),
              unnest_output_type->nameOf(replicate_idx)));
      replicate_idx++;
    } else {
      // Unnest source column: placeholder to maintain index alignment.
      // Use a UNIQUE dummy name to avoid collision with unnest output names.
      proj_names.push_back("__unnest_placeholder_" + std::to_string(i));
      proj_exprs.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              unnest_output_type->childAt(0),
              unnest_output_type->nameOf(0)));
    }
  }
  // Append unnest output columns (after all input columns).
  for (size_t i = replicate_idx; i < unnest_output_type->size(); ++i) {
    proj_names.push_back(unnest_output_type->nameOf(i));
    proj_exprs.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            unnest_output_type->childAt(i), unnest_output_type->nameOf(i)));
  }

  return std::make_shared<facebook::velox::core::ProjectNode>(
      nextNodeId(),
      std::move(proj_names),
      std::move(proj_exprs),
      unnest_node);
}

// ---------------------------------------------------------------------------
// Vector Search Extension → Project(distance) + TopN
//
// JSON detail: {"op":"vector_search","vectorColumn":"embedding",
//   "queryVector":[0.1,0.2,...],"topK":10,"metric":"L2"}
//
// Builds: input → Project(all_cols + _distance) → TopN(by _distance, k)
// Uses the already-registered Velox scalar functions (l2_distance,
// cosine_distance, inner_product) for distance computation.
// ---------------------------------------------------------------------------

namespace {

std::string parseJsonString(const std::string& json, const std::string& key) {
  auto key_str = "\"" + key + "\":\"";
  auto pos = json.find(key_str);
  if (pos == std::string::npos) return "";
  auto val_start = pos + key_str.size();
  auto val_end = json.find('"', val_start);
  if (val_end == std::string::npos) return "";
  return json.substr(val_start, val_end - val_start);
}

int64_t parseJsonInt(const std::string& json, const std::string& key) {
  auto key_str = "\"" + key + "\":";
  auto pos = json.find(key_str);
  if (pos == std::string::npos) return 0;
  auto val_start = pos + key_str.size();
  return std::stoll(json.substr(val_start));
}

std::vector<double> parseJsonDoubleArray(const std::string& json, const std::string& key) {
  auto key_str = "\"" + key + "\":[";
  auto pos = json.find(key_str);
  if (pos == std::string::npos) return {};
  auto arr_start = pos + key_str.size();
  auto arr_end = json.find(']', arr_start);
  if (arr_end == std::string::npos) return {};
  std::string arr = json.substr(arr_start, arr_end - arr_start);
  std::vector<double> result;
  std::istringstream ss(arr);
  std::string token;
  while (std::getline(ss, token, ',')) {
    if (!token.empty()) {
      result.push_back(std::stod(token));
    }
  }
  return result;
}

std::string metricToFunctionName(const std::string& metric) {
  if (metric == "COSINE") return "cosine_distance";
  if (metric == "INNER_PRODUCT") return "inner_product";
  return "l2_distance";
}

}  // namespace

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertVectorSearchExtension(
    const std::string& detail_json,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const facebook::velox::RowTypePtr& input_type) {
  std::string vector_column = parseJsonString(detail_json, "vectorColumn");
  int64_t top_k = parseJsonInt(detail_json, "topK");
  std::string metric = parseJsonString(detail_json, "metric");
  std::vector<double> query_vector = parseJsonDoubleArray(detail_json, "queryVector");

  if (vector_column.empty() || top_k <= 0 || query_vector.empty()) {
    throw std::runtime_error(
        "VeloxPlanConverter: invalid vector_search detail: vectorColumn, topK, "
        "and queryVector are required");
  }

  std::string dist_fn_name = metricToFunctionName(metric);

  // Build constant array expression for the query vector.
  auto array_type = facebook::velox::ARRAY(facebook::velox::DOUBLE());
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> element_exprs;
  element_exprs.reserve(query_vector.size());
  for (double val : query_vector) {
    element_exprs.push_back(
        std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::DOUBLE(),
            facebook::velox::variant(val)));
  }

  auto query_array_expr =
      std::make_shared<facebook::velox::core::CallTypedExpr>(
          array_type,
          std::move(element_exprs),
          "array_constructor");

  // Build field reference for the vector column.
  auto vec_col_expr =
      std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
          input_type->findChild(vector_column),
          vector_column);

  // Build distance function call: dist_fn(vector_col, query_array).
  auto dist_expr =
      std::make_shared<facebook::velox::core::CallTypedExpr>(
          facebook::velox::DOUBLE(),
          std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>{
              vec_col_expr, query_array_expr},
          dist_fn_name);

  // Build Project: pass through all input columns + add _distance column.
  std::vector<std::string> proj_names;
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> proj_exprs;
  for (size_t i = 0; i < input_type->size(); ++i) {
    proj_names.push_back(input_type->nameOf(i));
    proj_exprs.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            input_type->childAt(i), input_type->nameOf(i)));
  }
  proj_names.push_back("_distance");
  proj_exprs.push_back(dist_expr);

  auto project_node = std::make_shared<facebook::velox::core::ProjectNode>(
      nextNodeId(),
      std::move(proj_names),
      std::move(proj_exprs),
      input);

  // Build TopN: sort by _distance ASC, limit topK.
  auto project_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      project_node->outputType());

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      sorting_keys = {
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              facebook::velox::DOUBLE(), "_distance")};

  std::vector<facebook::velox::core::SortOrder> sorting_orders = {
      facebook::velox::core::SortOrder(true, true)};

  int32_t k = static_cast<int32_t>(std::min(top_k, static_cast<int64_t>(std::numeric_limits<int32_t>::max())));

  auto partial_topn = std::make_shared<facebook::velox::core::TopNNode>(
      nextNodeId(),
      sorting_keys,
      sorting_orders,
      k,
      /*isPartial=*/true,
      project_node);

  return std::make_shared<facebook::velox::core::TopNNode>(
      nextNodeId(),
      sorting_keys,
      sorting_orders,
      k,
      /*isPartial=*/false,
      partial_topn);
}

// ---------------------------------------------------------------------------
// HashJoinRel → HashJoinNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertHashJoinRel(
    const substrait::HashJoinRel& hash_join_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto left = convertRel(hash_join_rel.left(), splits);
  auto right = convertRel(hash_join_rel.right(), splits);

  auto left_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      left->outputType());
  auto right_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      right->outputType());

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      left_keys;
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      right_keys;

  for (const auto& key : hash_join_rel.left_keys()) {
    auto expr = convertFieldReference(key, left_type);
    auto field = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(expr);
    if (field) left_keys.push_back(field);
  }
  for (const auto& key : hash_join_rel.right_keys()) {
    auto expr = convertFieldReference(key, right_type);
    auto field = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(expr);
    if (field) right_keys.push_back(field);
  }

  auto join_type = mapHashJoinType(hash_join_rel.type());

  // Build output type with duplicate column name deduplication.
  // Use a set of ALL names seen so far (not just left) to handle chained joins
  // where a previous join already introduced "_right" suffixed names.
  std::vector<std::string> output_names;
  std::vector<facebook::velox::TypePtr> output_types;
  std::unordered_set<std::string> left_name_set;
  std::unordered_set<std::string> all_names;
  for (size_t i = 0; i < left_type->size(); ++i) {
    output_names.push_back(left_type->nameOf(i));
    output_types.push_back(left_type->childAt(i));
    left_name_set.insert(left_type->nameOf(i));
    all_names.insert(left_type->nameOf(i));
  }
  // Map from original right column name → deduplicated name for key renaming.
  std::unordered_map<std::string, std::string> right_rename_map;
  for (size_t i = 0; i < right_type->size(); ++i) {
    std::string orig = right_type->nameOf(i);
    std::string name = orig;
    if (all_names.count(name) > 0) {
      name = orig + "_right";
      int suffix = 2;
      while (all_names.count(name) > 0) {
        name = orig + "_right" + std::to_string(suffix++);
      }
    }
    right_rename_map[orig] = name;
    output_names.push_back(name);
    output_types.push_back(right_type->childAt(i));
    all_names.insert(name);
  }
  auto output_type =
      facebook::velox::ROW(std::move(output_names), std::move(output_types));

  std::shared_ptr<const facebook::velox::core::ITypedExpr> filter;
  if (hash_join_rel.has_post_join_filter()) {
    filter = convertExpression(hash_join_rel.post_join_filter(), output_type);
  }

  // Velox HashJoinNode::validate() requires that left and right input types
  // have no overlapping column names.  For self-joins (same table on both
  // sides), insert a ProjectNode to rename the right side's columns.
  bool hasOverlap = false;
  for (size_t i = 0; i < right_type->size() && !hasOverlap; ++i) {
    if (left_name_set.count(right_type->nameOf(i)) > 0) {
      hasOverlap = true;
    }
  }

  if (hasOverlap) {
    std::vector<std::string> renamed_names;
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> projections;
    renamed_names.reserve(right_type->size());
    projections.reserve(right_type->size());
    for (size_t i = 0; i < right_type->size(); ++i) {
      std::string orig_name = right_type->nameOf(i);
      renamed_names.push_back(right_rename_map[orig_name]);
      projections.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              right_type->childAt(i), orig_name));
    }
    auto rename_project = std::make_shared<facebook::velox::core::ProjectNode>(
        nextNodeId(),
        renamed_names,
        projections,
        right);

    std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
        renamed_right_keys;
    for (const auto& rk : right_keys) {
      std::string new_name = right_rename_map.count(rk->name())
          ? right_rename_map[rk->name()] : rk->name();
      renamed_right_keys.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              rk->type(), new_name));
    }

    return std::make_shared<facebook::velox::core::HashJoinNode>(
        nextNodeId(),
        join_type,
        /*nullAware=*/false,
        left_keys,
        renamed_right_keys,
        filter,
        left,
        rename_project,
        output_type);
  }

  return std::make_shared<facebook::velox::core::HashJoinNode>(
      nextNodeId(),
      join_type,
      /*nullAware=*/false,
      left_keys,
      right_keys,
      filter,
      left,
      right,
      output_type);
}

// ---------------------------------------------------------------------------
// JoinRel helpers
// ---------------------------------------------------------------------------

facebook::velox::RowTypePtr VeloxPlanConverter::buildJoinOutputType(
    const facebook::velox::RowTypePtr& left_type,
    const facebook::velox::RowTypePtr& right_type,
    std::unordered_set<std::string>& left_name_set,
    std::unordered_map<std::string, std::string>& right_rename_map) {
  // Deduplicate right-side column names to avoid Velox RowType validation
  // failure when left and right tables share column names.
  std::vector<std::string> output_names;
  std::vector<facebook::velox::TypePtr> output_types;
  std::unordered_set<std::string> all_names;
  for (size_t i = 0; i < left_type->size(); ++i) {
    output_names.push_back(left_type->nameOf(i));
    output_types.push_back(left_type->childAt(i));
    left_name_set.insert(left_type->nameOf(i));
    all_names.insert(left_type->nameOf(i));
  }
  for (size_t i = 0; i < right_type->size(); ++i) {
    std::string orig = right_type->nameOf(i);
    std::string name = orig;
    if (all_names.count(name) > 0) {
      name = orig + "_right";
      int suffix = 2;
      while (all_names.count(name) > 0) {
        name = orig + "_right" + std::to_string(suffix++);
      }
    }
    right_rename_map[orig] = name;
    output_names.push_back(name);
    output_types.push_back(right_type->childAt(i));
    all_names.insert(name);
  }
  return facebook::velox::ROW(std::move(output_names), std::move(output_types));
}

void VeloxPlanConverter::extractJoinKeys(
    const substrait::Expression& expr,
    const facebook::velox::RowTypePtr& left_type,
    const facebook::velox::RowTypePtr& right_type,
    const facebook::velox::RowTypePtr& output_type,
    int left_field_count,
    std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& left_keys,
    std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& right_keys,
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& residual_conditions) {
  auto getFieldIndex = [](const substrait::Expression& e) -> int {
    if (e.has_selection() &&
        e.selection().has_direct_reference() &&
        e.selection().direct_reference().has_struct_field()) {
      return e.selection().direct_reference().struct_field().field();
    }
    return -1;
  };

  std::function<void(const substrait::Expression&)> extract;
  extract = [&](const substrait::Expression& e) {
    if (e.has_scalar_function()) {
      const auto& func = e.scalar_function();
      auto it = function_map_.find(func.function_reference());
      std::string fname = (it != function_map_.end()) ? it->second : "";
      if (fname == "and" && func.arguments_size() == 2) {
        extract(func.arguments(0).value());
        extract(func.arguments(1).value());
        return;
      }
      if ((fname == "equal" || fname == "eq" ||
           fname == "is_not_distinct_from") &&
          func.arguments_size() == 2) {
        int lhs = getFieldIndex(func.arguments(0).value());
        int rhs = getFieldIndex(func.arguments(1).value());
        if (lhs >= 0 && rhs >= 0) {
          int li = -1, ri = -1;
          if (lhs < left_field_count && rhs >= left_field_count) {
            li = lhs; ri = rhs - left_field_count;
          } else if (rhs < left_field_count && lhs >= left_field_count) {
            li = rhs; ri = lhs - left_field_count;
          }
          if (li >= 0 && ri >= 0 &&
              li < static_cast<int>(left_type->size()) &&
              ri < static_cast<int>(right_type->size())) {
            left_keys.push_back(std::make_shared<
                facebook::velox::core::FieldAccessTypedExpr>(
                left_type->childAt(li), left_type->nameOf(li)));
            right_keys.push_back(std::make_shared<
                facebook::velox::core::FieldAccessTypedExpr>(
                right_type->childAt(ri), right_type->nameOf(ri)));
            return;
          }
        }
      }
    }
    residual_conditions.push_back(convertExpression(e, output_type));
  };
  extract(expr);
}

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::buildJoinProjection(
    const facebook::velox::RowTypePtr& right_type,
    const std::unordered_set<std::string>& left_name_set,
    const std::unordered_map<std::string, std::string>& right_rename_map,
    const std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& right_keys,
    std::shared_ptr<const facebook::velox::core::PlanNode> right,
    std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& renamed_right_keys) {
  // Check if right-side columns overlap with left-side names.
  bool hasOverlap = false;
  for (size_t i = 0; i < right_type->size() && !hasOverlap; ++i) {
    if (left_name_set.count(right_type->nameOf(i)) > 0) hasOverlap = true;
  }
  if (!hasOverlap) return nullptr;

  // Build a ProjectNode that renames right-side columns.
  std::vector<std::string> renamed_names;
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> projections;
  renamed_names.reserve(right_type->size());
  projections.reserve(right_type->size());
  for (size_t i = 0; i < right_type->size(); ++i) {
    std::string orig = right_type->nameOf(i);
    renamed_names.push_back(right_rename_map.at(orig));
    projections.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            right_type->childAt(i), orig));
  }
  auto rename_project = std::make_shared<facebook::velox::core::ProjectNode>(
      nextNodeId(), renamed_names, projections, right);

  // Rebuild right_keys to reference the renamed columns.
  for (const auto& rk : right_keys) {
    auto it = right_rename_map.find(rk->name());
    std::string new_name = (it != right_rename_map.end()) ? it->second : rk->name();
    renamed_right_keys.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            rk->type(), new_name));
  }
  return rename_project;
}

// ---------------------------------------------------------------------------
// JoinRel → HashJoinNode / MergeJoinNode / NestedLoopJoinNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertJoinRel(
    const substrait::JoinRel& join_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto left = convertRel(join_rel.left(), splits);
  auto right = convertRel(join_rel.right(), splits);
  auto left_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      left->outputType());
  auto right_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      right->outputType());
  int left_field_count = static_cast<int>(left_type->size());
  auto join_type = mapJoinType(join_rel.type());

  std::unordered_set<std::string> left_name_set;
  std::unordered_map<std::string, std::string> right_rename_map;
  // ANTI and LEFT SEMI joins produce only left-side columns.
  // Velox HashJoinNode validates that the output type matches the join
  // semantics — including right-side columns for ANTI/SEMI causes
  // validation failure or silent 0-row output.
  facebook::velox::RowTypePtr output_type;
  if (join_type == facebook::velox::core::JoinType::kAnti ||
      join_type == facebook::velox::core::JoinType::kLeftSemiFilter ||
      join_type == facebook::velox::core::JoinType::kLeftSemiProject) {
    output_type = left_type;
    for (size_t i = 0; i < left_type->size(); ++i) {
      left_name_set.insert(left_type->nameOf(i));
    }
    // Populate right_rename_map for overlapping column names.
    // HashJoinNode requires unique names across both sides internally,
    // even though the output only contains left-side columns.
    for (size_t i = 0; i < right_type->size(); ++i) {
      std::string orig = right_type->nameOf(i);
      if (left_name_set.count(orig)) {
        right_rename_map[orig] = orig + "_right";
      }
    }
  } else {
    output_type = buildJoinOutputType(
        left_type, right_type, left_name_set, right_rename_map);
  }

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      left_keys, right_keys;
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
      residual_conditions;
  if (join_rel.has_expression()) {
    // For expression resolution, use the full combined type (left + right)
    // so that residual conditions can reference right-side columns.
    // The ANTI/SEMI output_type (left-only) is only for HashJoinNode output.
    std::unordered_set<std::string> expr_left_set;
    std::unordered_map<std::string, std::string> expr_rename_map;
    auto expr_type = buildJoinOutputType(
        left_type, right_type, expr_left_set, expr_rename_map);
    extractJoinKeys(join_rel.expression(), left_type, right_type,
                    expr_type, left_field_count,
                    left_keys, right_keys, residual_conditions);
  }

  // Build post-join filter from residual conditions.
  std::shared_ptr<const facebook::velox::core::ITypedExpr> filter;
  if (residual_conditions.size() == 1) {
    filter = residual_conditions[0];
  } else if (residual_conditions.size() > 1) {
    filter = residual_conditions[0];
    for (size_t i = 1; i < residual_conditions.size(); ++i) {
      filter = std::make_shared<facebook::velox::core::CallTypedExpr>(
          facebook::velox::BOOLEAN(),
          std::vector<std::shared_ptr<
              const facebook::velox::core::ITypedExpr>>{filter, residual_conditions[i]},
          "and");
    }
  }

  // Case 1: No equi-join keys → NestedLoopJoinNode.
  if (left_keys.empty()) {
    std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
        renamed_right_keys_unused;
    auto rename_project = buildJoinProjection(
        right_type, left_name_set, right_rename_map,
        right_keys, right, renamed_right_keys_unused);
    if (rename_project) {
      return std::make_shared<facebook::velox::core::NestedLoopJoinNode>(
          nextNodeId(), join_type, filter, left, rename_project, output_type);
    }
    return std::make_shared<facebook::velox::core::NestedLoopJoinNode>(
        nextNodeId(), join_type, filter, left, right, output_type);
  }

  // Case 2: Equi-join with sorted inputs → MergeJoinNode.
  if ((join_type == facebook::velox::core::JoinType::kInner ||
       join_type == facebook::velox::core::JoinType::kLeft) && !filter) {
    auto leftSort = std::dynamic_pointer_cast<
        const facebook::velox::core::OrderByNode>(left);
    auto rightSort = std::dynamic_pointer_cast<
        const facebook::velox::core::OrderByNode>(right);
    if (leftSort && rightSort &&
        leftSort->sortingKeys().size() >= left_keys.size() &&
        rightSort->sortingKeys().size() >= right_keys.size()) {
      bool keys_match = true;
      for (size_t i = 0; i < left_keys.size() && keys_match; ++i) {
        if (leftSort->sortingKeys()[i]->name() != left_keys[i]->name() ||
            rightSort->sortingKeys()[i]->name() != right_keys[i]->name())
          keys_match = false;
      }
      if (keys_match) {
        std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
            renamed_right_keys_merge;
        auto rename_project_merge = buildJoinProjection(
            right_type, left_name_set, right_rename_map,
            right_keys, rightSort->sources()[0], renamed_right_keys_merge);
        if (rename_project_merge) {
          return std::make_shared<facebook::velox::core::MergeJoinNode>(
              nextNodeId(), join_type, left_keys, renamed_right_keys_merge,
              /*filter=*/nullptr, leftSort->sources()[0],
              rename_project_merge, output_type);
        }
        return std::make_shared<facebook::velox::core::MergeJoinNode>(
            nextNodeId(), join_type, left_keys, right_keys,
            /*filter=*/nullptr, leftSort->sources()[0],
            rightSort->sources()[0], output_type);
      }
    }
  }

  // Case 3: Default → HashJoinNode (with optional right-side rename).
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      renamed_right_keys;
  auto rename_project = buildJoinProjection(
      right_type, left_name_set, right_rename_map,
      right_keys, right, renamed_right_keys);
  if (rename_project) {
    return std::make_shared<facebook::velox::core::HashJoinNode>(
        nextNodeId(), join_type, /*nullAware=*/false,
        left_keys, renamed_right_keys, filter,
        left, rename_project, output_type);
  }
  return std::make_shared<facebook::velox::core::HashJoinNode>(
      nextNodeId(), join_type, /*nullAware=*/false,
      left_keys, right_keys, filter, left, right, output_type);
}

// ---------------------------------------------------------------------------
// ExchangeRel → PartitionedOutputNode / ExchangeNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertExchangeRel(
    const substrait::ExchangeRel& exchange_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(exchange_rel.input(), splits);
  auto output_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());

  if (exchange_rel.has_scatter_by_fields()) {
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
        partition_keys;
    std::vector<facebook::velox::column_index_t> key_channels;
    for (const auto& field_ref :
         exchange_rel.scatter_by_fields().fields()) {
      if (field_ref.has_direct_reference() &&
          field_ref.direct_reference().has_struct_field()) {
        int idx = field_ref.direct_reference().struct_field().field();
        if (idx >= 0 &&
            idx < static_cast<int>(output_type->size())) {
          partition_keys.push_back(
              std::make_shared<
                  facebook::velox::core::FieldAccessTypedExpr>(
                  output_type->childAt(idx),
                  output_type->nameOf(idx)));
          key_channels.push_back(
              static_cast<facebook::velox::column_index_t>(idx));
        }
      }
    }
    int num_partitions = exchange_rel.partition_count() > 0
        ? exchange_rel.partition_count()
        : 1;
    return std::make_shared<facebook::velox::core::PartitionedOutputNode>(
        nextNodeId(),
        facebook::velox::core::PartitionedOutputNode::Kind::kPartitioned,
        partition_keys,
        num_partitions,
        /*replicateNullsAndAny=*/false,
        std::make_shared<facebook::velox::exec::HashPartitionFunctionSpec>(
            output_type, std::move(key_channels)),
        output_type,
        std::string(kDefaultSerdeFormat),
        input);
  }

  // Default: ExchangeNode represents a shuffle read boundary.
  return std::make_shared<facebook::velox::core::ExchangeNode>(
      nextNodeId(), output_type, std::string(kDefaultSerdeFormat));
}

// ---------------------------------------------------------------------------
// SortOrder conversion helper
// ---------------------------------------------------------------------------

facebook::velox::core::SortOrder VeloxPlanConverter::convertSortOrder(
    const substrait::SortField& sort_field) {
  bool ascending = true;
  bool nulls_first = true;
  if (sort_field.has_direction()) {
    switch (sort_field.direction()) {
      case substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST:
        ascending = true; nulls_first = true; break;
      case substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST:
        ascending = true; nulls_first = false; break;
      case substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST:
        ascending = false; nulls_first = true; break;
      case substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST:
        ascending = false; nulls_first = false; break;
      default:
        break;
    }
  }
  return facebook::velox::core::SortOrder(ascending, nulls_first);
}

// ---------------------------------------------------------------------------
// SortRel → OrderByNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertSortRel(
    const substrait::SortRel& sort_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(sort_rel.input(), splits);
  auto input_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      sorting_keys;
  std::vector<facebook::velox::core::SortOrder> sorting_orders;

  for (const auto& sort_field : sort_rel.sorts()) {
    auto expr = convertExpression(sort_field.expr(), input_type);
    auto field = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(expr);
    if (field) {
      sorting_keys.push_back(field);
      sorting_orders.push_back(convertSortOrder(sort_field));
    }
  }

  // Note: OrderByNode is NOT split into partial→final here because
  // SortRel is almost always followed by FetchRel (ORDER BY + LIMIT).
  // convertFetchRel detects OrderByNode as input and optimizes to
  // TopNNode(partial=true) → TopNNode(partial=false), which handles
  // parallel-safe sorting with limit.  Splitting OrderBy here would
  // conflict with that optimization.
  return std::make_shared<facebook::velox::core::OrderByNode>(
      nextNodeId(),
      sorting_keys,
      sorting_orders,
      /*isPartial=*/false,
      input);
}

// ---------------------------------------------------------------------------
// FetchRel → LimitNode / TopNNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertFetchRel(
    const substrait::FetchRel& fetch_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(fetch_rel.input(), splits);

  int64_t offset = fetch_rel.offset();
  int64_t count = fetch_rel.count();

  // P2-BUG-13: Guard against int32 overflow when casting offset/count.
  // Velox LimitNode/TopNNode use int32_t; clamp to INT32_MAX to avoid
  // silent truncation producing negative or incorrect values.
  constexpr int64_t kMaxInt32 = static_cast<int64_t>(std::numeric_limits<int32_t>::max());
  if (offset > kMaxInt32) {
    offset = kMaxInt32;
  }
  if (count > kMaxInt32) {
    count = kMaxInt32;
  }

  // If the input is an OrderByNode, optimize to TopNNode.
  // Parallel-safe TopN: split into isPartial=true → isPartial=false.
  // In Velox parallel mode, each driver independently executes the plan.
  // A single TopNNode(isPartial=false) produces per-driver top-N results
  // that are concatenated without global merge — yielding N×max_drivers
  // rows in arbitrary order.  Splitting into partial + final ensures each
  // driver emits its local top-N (partial), then a single-threaded final
  // TopNNode merges all partial results into the correct global top-N.
  // This is the same pattern as kPartial→kFinal for AggregationNode.
  auto order_by = std::dynamic_pointer_cast<
      const facebook::velox::core::OrderByNode>(input);
  if (order_by && offset == 0) {
    auto partialTopN = std::make_shared<facebook::velox::core::TopNNode>(
        nextNodeId(),
        order_by->sortingKeys(),
        order_by->sortingOrders(),
        static_cast<int32_t>(count),
        /*isPartial=*/true,
        order_by->sources()[0]);
    return std::make_shared<facebook::velox::core::TopNNode>(
        nextNodeId(),
        order_by->sortingKeys(),
        order_by->sortingOrders(),
        static_cast<int32_t>(count),
        /*isPartial=*/false,
        partialTopN);
  }

  // Parallel-safe Limit: split into isPartial=true → isPartial=false.
  // Same rationale as TopN above — without partial+final, each driver
  // independently applies the limit, producing N×max_drivers rows total.
  auto partialLimit = std::make_shared<facebook::velox::core::LimitNode>(
      nextNodeId(),
      static_cast<int32_t>(offset),
      static_cast<int32_t>(count),
      /*isPartial=*/true,
      input);
  return std::make_shared<facebook::velox::core::LimitNode>(
      nextNodeId(),
      0,  // offset already applied in partial stage
      static_cast<int32_t>(count),
      /*isPartial=*/false,
      partialLimit);
}

// ---------------------------------------------------------------------------
// WindowRel → WindowNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertWindowRel(
    const substrait::ConsistentPartitionWindowRel& window_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(window_rel.input(), splits);
  auto input_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());

  // Convert partition keys.
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      partition_keys;
  for (const auto& expr : window_rel.partition_expressions()) {
    auto converted = convertExpression(expr, input_type);
    auto field = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(converted);
    if (field) {
      partition_keys.push_back(field);
    }
  }

  // Convert sorting keys.
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      sorting_keys;
  std::vector<facebook::velox::core::SortOrder> sorting_orders;
  for (const auto& sort_field : window_rel.sorts()) {
    auto expr = convertExpression(sort_field.expr(), input_type);
    auto field = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(expr);
    if (field) {
      sorting_keys.push_back(field);
      sorting_orders.push_back(convertSortOrder(sort_field));
    }
  }

  // RowNumberNode optimization: when the window contains only a single
  // row_number() with no custom frame and no other window functions,
  // use the specialized RowNumberNode which is more efficient than
  // the general-purpose WindowNode.
  if (isRowNumberOnlyPattern(window_rel, function_map_)) {
    auto rn_input = input;
    // RowNumberNode assigns consecutive numbers in arrival order per partition.
    // When ORDER BY is specified, sort by (partition_keys, sorting_keys) so
    // row numbers follow the requested ordering within each partition.
    if (!sorting_keys.empty()) {
      std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>> all_keys;
      std::vector<facebook::velox::core::SortOrder> all_orders;
      for (const auto& pk : partition_keys) {
        all_keys.push_back(pk);
        all_orders.push_back(facebook::velox::core::SortOrder(true, true));
      }
      for (size_t i = 0; i < sorting_keys.size(); ++i) {
        all_keys.push_back(sorting_keys[i]);
        all_orders.push_back(sorting_orders[i]);
      }
      rn_input = std::make_shared<facebook::velox::core::OrderByNode>(
          nextNodeId(), all_keys, all_orders, false, rn_input);
    }
    auto node_id = nextNodeId();
    auto rn_node = std::make_shared<facebook::velox::core::RowNumberNode>(
        node_id,
        partition_keys,
        /*rowNumberColumnName=*/std::make_optional(std::string("row_number")),
        /*limit=*/std::nullopt,
        rn_input);
    if (!sorting_keys.empty()) {
      row_number_sort_info_[node_id] = {sorting_keys, sorting_orders};
    }
    return rn_node;
  }

  // Fall through to existing WindowNode path for non-matching patterns.

  // Convert window functions.
  std::vector<std::string> window_column_names;
  std::vector<facebook::velox::core::WindowNode::Function> window_functions;

  for (int i = 0; i < window_rel.window_functions_size(); ++i) {
    const auto& wf = window_rel.window_functions(i);
    window_column_names.push_back("window_" + std::to_string(i));

    facebook::velox::core::WindowNode::Function func;

    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> args;
    for (const auto& arg : wf.arguments()) {
      if (arg.has_value()) {
        args.push_back(convertExpression(arg.value(), input_type));
      }
    }
    auto output_type = convertType(wf.output_type());
    std::string func_name;
    auto it = function_map_.find(wf.function_reference());
    if (it != function_map_.end()) {
      func_name = it->second;
    } else {
      func_name = "window_func_" + std::to_string(wf.function_reference());
    }
    func.functionCall = std::make_shared<facebook::velox::core::CallTypedExpr>(
        output_type, std::move(args), func_name);

    // Build window frame.
    auto mapBound = [](
        const substrait::Expression_WindowFunction_Bound& bound,
        bool isLower)
        -> facebook::velox::core::WindowNode::BoundType {
      if (bound.has_unbounded()) {
        return isLower
            ? facebook::velox::core::WindowNode::BoundType::kUnboundedPreceding
            : facebook::velox::core::WindowNode::BoundType::kUnboundedFollowing;
      }
      if (bound.has_current_row()) {
        return facebook::velox::core::WindowNode::BoundType::kCurrentRow;
      }
      if (bound.has_preceding()) {
        return facebook::velox::core::WindowNode::BoundType::kPreceding;
      }
      if (bound.has_following()) {
        return facebook::velox::core::WindowNode::BoundType::kFollowing;
      }
      return facebook::velox::core::WindowNode::BoundType::kCurrentRow;
    };

    func.frame.type = (wf.bounds_type() == 2)
        ? facebook::velox::core::WindowNode::WindowType::kRange
        : facebook::velox::core::WindowNode::WindowType::kRows;
    func.frame.startType = mapBound(wf.lower_bound(), /*isLower=*/true);
    func.frame.endType = mapBound(wf.upper_bound(), /*isLower=*/false);
    func.frame.startValue = nullptr;
    func.frame.endValue = nullptr;

    // Extract bound offset values for PRECEDING/FOLLOWING.
    if (wf.has_lower_bound()) {
      if (wf.lower_bound().has_preceding()) {
        int64_t offset_val = wf.lower_bound().preceding().offset();
        func.frame.startValue =
            std::make_shared<facebook::velox::core::ConstantTypedExpr>(
                facebook::velox::BIGINT(),
                facebook::velox::variant(offset_val));
      } else if (wf.lower_bound().has_following()) {
        int64_t offset_val = wf.lower_bound().following().offset();
        func.frame.startValue =
            std::make_shared<facebook::velox::core::ConstantTypedExpr>(
                facebook::velox::BIGINT(),
                facebook::velox::variant(offset_val));
      }
    }
    if (wf.has_upper_bound()) {
      if (wf.upper_bound().has_following()) {
        int64_t offset_val = wf.upper_bound().following().offset();
        func.frame.endValue =
            std::make_shared<facebook::velox::core::ConstantTypedExpr>(
                facebook::velox::BIGINT(),
                facebook::velox::variant(offset_val));
      } else if (wf.upper_bound().has_preceding()) {
        int64_t offset_val = wf.upper_bound().preceding().offset();
        func.frame.endValue =
            std::make_shared<facebook::velox::core::ConstantTypedExpr>(
                facebook::velox::BIGINT(),
                facebook::velox::variant(offset_val));
      }
    }

    func.ignoreNulls = false;
    window_functions.push_back(std::move(func));
  }

  // Parallel-safe WindowNode: gather all data into a single driver
  // before computing window functions. See convertProjectWithWindowFunctions
  // for detailed rationale.
  auto gathered = facebook::velox::core::LocalPartitionNode::gather(
      nextNodeId(),
      std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>>{
          input});

  return std::make_shared<facebook::velox::core::WindowNode>(
      nextNodeId(),
      partition_keys,
      sorting_keys,
      sorting_orders,
      window_column_names,
      window_functions,
      /*inputsSorted=*/false,
      gathered);
}

// ---------------------------------------------------------------------------
// SetRel helpers
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::buildUnionAll(
    std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>> sources) {
  return facebook::velox::core::LocalPartitionNode::gather(
      nextNodeId(), std::move(sources));
}

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::buildIntersectOrExcept(
    std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>> sources,
    facebook::velox::core::JoinType join_type) {
  // INTERSECT via chained left semi-joins, EXCEPT via chained anti-joins.
  // Velox HashJoinNode rejects duplicate column names across left/right
  // sides, so we wrap the right source in a ProjectNode that renames
  // columns to unique aliases.
  auto result = sources[0];
  for (size_t s = 1; s < sources.size(); ++s) {
    auto left_t = std::dynamic_pointer_cast<
        const facebook::velox::RowType>(result->outputType());
    auto right_t = std::dynamic_pointer_cast<
        const facebook::velox::RowType>(sources[s]->outputType());

    std::vector<std::string> renamed_names;
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
        rename_exprs;
    renamed_names.reserve(right_t->size());
    rename_exprs.reserve(right_t->size());
    for (size_t i = 0; i < right_t->size(); ++i) {
      renamed_names.push_back(
          "set_r_" + std::to_string(s) + "_" + std::to_string(i));
      rename_exprs.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              right_t->childAt(i), right_t->nameOf(i)));
    }
    auto renamed_right =
        std::make_shared<facebook::velox::core::ProjectNode>(
            nextNodeId(), std::move(renamed_names),
            std::move(rename_exprs), sources[s]);
    auto renamed_right_t = std::dynamic_pointer_cast<
        const facebook::velox::RowType>(renamed_right->outputType());

    std::vector<std::shared_ptr<
        const facebook::velox::core::FieldAccessTypedExpr>> left_keys, right_keys;
    size_t key_count = std::min(left_t->size(), renamed_right_t->size());
    for (size_t i = 0; i < key_count; ++i) {
      left_keys.push_back(std::make_shared<
          facebook::velox::core::FieldAccessTypedExpr>(
          left_t->childAt(i), left_t->nameOf(i)));
      right_keys.push_back(std::make_shared<
          facebook::velox::core::FieldAccessTypedExpr>(
          renamed_right_t->childAt(i), renamed_right_t->nameOf(i)));
    }
    result = std::make_shared<facebook::velox::core::HashJoinNode>(
        nextNodeId(), join_type, /*nullAware=*/false,
        left_keys, right_keys, /*filter=*/nullptr,
        result, renamed_right, left_t,
        /*useHashTableCache=*/false, /*nullAsValue=*/true);
  }
  return result;
}

// ---------------------------------------------------------------------------
// SetRel → UNION ALL / INTERSECT / EXCEPT
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertSetRel(
    const substrait::SetRel& set_rel,
    const std::vector<split::SplitConfig>& splits) {
  if (set_rel.inputs_size() == 0) {
    throw std::runtime_error("VeloxPlanConverter: SetRel has no inputs");
  }

  std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>> sources;
  for (int i = 0; i < set_rel.inputs_size(); ++i) {
    sources.push_back(convertRel(set_rel.inputs(i), splits));
  }
  if (sources.size() == 1) {
    return sources[0];
  }

  switch (set_rel.op()) {
    case substrait::SetRel::SET_OP_UNION_ALL:
      return buildUnionAll(std::move(sources));

    case substrait::SetRel::SET_OP_INTERSECTION_PRIMARY:
    case substrait::SetRel::SET_OP_INTERSECTION_MULTISET:
      return buildIntersectOrExcept(
          std::move(sources),
          facebook::velox::core::JoinType::kLeftSemiFilter);

    case substrait::SetRel::SET_OP_MINUS_PRIMARY:
    case substrait::SetRel::SET_OP_MINUS_MULTISET:
      return buildIntersectOrExcept(
          std::move(sources),
          facebook::velox::core::JoinType::kAnti);

    default:
      return buildUnionAll(std::move(sources));
  }
}

// ---------------------------------------------------------------------------
// Window helpers (for convertProjectWithWindowFunctions)
// ---------------------------------------------------------------------------

namespace {

// Map Substrait window bound to Velox BoundType.
facebook::velox::core::WindowNode::BoundType mapWindowBound(
    const substrait::Expression_WindowFunction_Bound& bound,
    bool isLower) {
  if (bound.has_unbounded()) {
    return isLower
        ? facebook::velox::core::WindowNode::BoundType::kUnboundedPreceding
        : facebook::velox::core::WindowNode::BoundType::kUnboundedFollowing;
  }
  if (bound.has_current_row())
    return facebook::velox::core::WindowNode::BoundType::kCurrentRow;
  if (bound.has_preceding())
    return facebook::velox::core::WindowNode::BoundType::kPreceding;
  if (bound.has_following())
    return facebook::velox::core::WindowNode::BoundType::kFollowing;
  return facebook::velox::core::WindowNode::BoundType::kCurrentRow;
}

// Extract a bound offset value as a ConstantTypedExpr, or nullptr.
std::shared_ptr<const facebook::velox::core::ConstantTypedExpr>
extractBoundOffset(
    const substrait::Expression_WindowFunction_Bound& bound,
    bool isStart) {
  if (isStart) {
    if (bound.has_preceding())
      return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
          facebook::velox::BIGINT(),
          facebook::velox::variant(bound.preceding().offset()));
    if (bound.has_following())
      return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
          facebook::velox::BIGINT(),
          facebook::velox::variant(bound.following().offset()));
  } else {
    if (bound.has_following())
      return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
          facebook::velox::BIGINT(),
          facebook::velox::variant(bound.following().offset()));
    if (bound.has_preceding())
      return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
          facebook::velox::BIGINT(),
          facebook::velox::variant(bound.preceding().offset()));
  }
  return nullptr;
}

}  // namespace

std::vector<VeloxPlanConverter::WindowFuncInfo>
VeloxPlanConverter::extractWindowFunctions(
    const substrait::ProjectRel& project_rel) {
  std::vector<WindowFuncInfo> result;
  for (int i = 0; i < project_rel.expressions_size(); ++i) {
    if (project_rel.expressions(i).has_window_function()) {
      result.push_back(
          {i, &project_rel.expressions(i).window_function()});
    }
  }
  return result;
}

std::vector<VeloxPlanConverter::WindowGroup>
VeloxPlanConverter::groupWindowFunctions(
    const std::vector<WindowFuncInfo>& window_funcs) {
  auto buildSignature = [](const substrait::Expression_WindowFunction& wf)
      -> std::string {
    std::string sig = "P:";
    for (int p = 0; p < wf.partitions_size(); ++p)
      sig += wf.partitions(p).SerializeAsString() + ",";
    sig += "S:";
    for (int s = 0; s < wf.sorts_size(); ++s)
      sig += wf.sorts(s).SerializeAsString() + ",";
    return sig;
  };

  std::vector<WindowGroup> groups;
  std::unordered_map<std::string, size_t> sig_to_group;
  for (const auto& wfi : window_funcs) {
    auto sig = buildSignature(*wfi.wf);
    auto it = sig_to_group.find(sig);
    if (it == sig_to_group.end()) {
      sig_to_group[sig] = groups.size();
      groups.push_back({sig, {wfi}});
    } else {
      groups[it->second].funcs.push_back(wfi);
    }
  }
  return groups;
}

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::buildWindowNode(
    const WindowGroup& group,
    const facebook::velox::RowTypePtr& current_type,
    std::shared_ptr<const facebook::velox::core::PlanNode> current_input,
    std::unordered_map<int, std::string>& proj_index_to_window_col) {
  const auto& rep = *group.funcs[0].wf;

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      partition_keys;
  for (int p = 0; p < rep.partitions_size(); ++p) {
    auto f = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(
        convertExpression(rep.partitions(p), current_type));
    if (f) partition_keys.push_back(f);
  }
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      sorting_keys;
  std::vector<facebook::velox::core::SortOrder> sorting_orders;
  for (int s = 0; s < rep.sorts_size(); ++s) {
    auto f = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(
        convertExpression(rep.sorts(s).expr(), current_type));
    if (f) {
      sorting_keys.push_back(f);
      sorting_orders.push_back(convertSortOrder(rep.sorts(s)));
    }
  }

  // RowNumberNode optimization: when the group contains only a single
  // row_number() with no custom frame, use the specialized RowNumberNode.
  if (group.funcs.size() == 1 &&
      isRowNumberOnlyWindowFunc(*group.funcs[0].wf, function_map_)) {
    // Map the projection index to the RowNumberNode output column name.
    // RowNumberNode appends a "row_number" column to the input schema.
    std::string rn_col = "row_number";
    proj_index_to_window_col[group.funcs[0].proj_index] = rn_col;

    auto rn_input = insertLocalPartitionExchange(current_input, partition_keys);
    if (!sorting_keys.empty()) {
      std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>> all_keys;
      std::vector<facebook::velox::core::SortOrder> all_orders;
      for (const auto& pk : partition_keys) {
        all_keys.push_back(pk);
        all_orders.push_back(facebook::velox::core::SortOrder(true, true));
      }
      for (size_t i = 0; i < sorting_keys.size(); ++i) {
        all_keys.push_back(sorting_keys[i]);
        all_orders.push_back(sorting_orders[i]);
      }
      rn_input = std::make_shared<facebook::velox::core::OrderByNode>(
          nextNodeId(), all_keys, all_orders, false, rn_input);
    }
    auto node_id = nextNodeId();
    auto rn_node = std::make_shared<facebook::velox::core::RowNumberNode>(
        node_id,
        partition_keys,
        /*rowNumberColumnName=*/std::make_optional(rn_col),
        /*limit=*/std::nullopt,
        rn_input);
    if (!sorting_keys.empty()) {
      row_number_sort_info_[node_id] = {sorting_keys, sorting_orders};
    }
    return rn_node;
  }

  // Fall through to existing WindowNode path.
  std::vector<std::string> col_names;
  std::vector<facebook::velox::core::WindowNode::Function> funcs;
  for (const auto& wfi : group.funcs) {
    const auto& wf = *wfi.wf;
    std::string cn = "w_" + std::to_string(wfi.proj_index);
    col_names.push_back(cn);
    proj_index_to_window_col[wfi.proj_index] = cn;

    facebook::velox::core::WindowNode::Function func;
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> args;
    for (const auto& arg : wf.arguments())
      if (arg.has_value())
        args.push_back(convertExpression(arg.value(), current_type));
    auto it = function_map_.find(wf.function_reference());
    std::string fname = (it != function_map_.end())
        ? it->second : "window_func_" + std::to_string(wf.function_reference());
    func.functionCall = std::make_shared<facebook::velox::core::CallTypedExpr>(
        convertType(wf.output_type()), std::move(args), fname);
    func.frame.type = (wf.bounds_type() == 2)
        ? facebook::velox::core::WindowNode::WindowType::kRange
        : facebook::velox::core::WindowNode::WindowType::kRows;
    func.frame.startType = mapWindowBound(wf.lower_bound(), true);
    func.frame.endType = mapWindowBound(wf.upper_bound(), false);
    func.frame.startValue = wf.has_lower_bound()
        ? extractBoundOffset(wf.lower_bound(), true) : nullptr;
    func.frame.endValue = wf.has_upper_bound()
        ? extractBoundOffset(wf.upper_bound(), false) : nullptr;
    func.ignoreNulls = false;
    funcs.push_back(std::move(func));
  }

  // Parallel-safe: gather all data into a single driver before computing
  // window functions (required for correct ROW_NUMBER, RANK, LEAD/LAG).
  auto gathered = facebook::velox::core::LocalPartitionNode::gather(
      nextNodeId(), {current_input});
  return std::make_shared<facebook::velox::core::WindowNode>(
      nextNodeId(), partition_keys, sorting_keys, sorting_orders,
      col_names, funcs, /*inputsSorted=*/false, gathered);
}

// ---------------------------------------------------------------------------
// ProjectRel with WindowFunction expressions → WindowNode + ProjectNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertProjectWithWindowFunctions(
    const substrait::ProjectRel& project_rel,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const facebook::velox::RowTypePtr& input_type) {
  auto window_funcs = extractWindowFunctions(project_rel);
  auto groups = groupWindowFunctions(window_funcs);

  std::unordered_map<int, std::string> proj_index_to_window_col;
  auto current_input = input;
  auto current_type = input_type;

  for (auto& group : groups) {
    current_input = buildWindowNode(
        group, current_type, current_input, proj_index_to_window_col);
    current_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
        current_input->outputType());
  }

  // Build the final ProjectNode mapping output columns to expected order.
  std::vector<std::string> names;
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> projections;
  names.reserve(project_rel.expressions_size());
  projections.reserve(project_rel.expressions_size());

  for (int i = 0; i < project_rel.expressions_size(); ++i) {
    auto wit = proj_index_to_window_col.find(i);
    if (wit != proj_index_to_window_col.end()) {
      const auto& col_name = wit->second;
      auto col_idx = current_type->getChildIdxIfExists(col_name);
      if (col_idx.has_value()) {
        projections.push_back(
            std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
                current_type->childAt(col_idx.value()), col_name));
        names.push_back("proj_" + std::to_string(i));
      } else {
        projections.push_back(
            convertExpression(project_rel.expressions(i), current_type));
        names.push_back("proj_" + std::to_string(i));
      }
    } else {
      auto expr = convertExpression(project_rel.expressions(i), current_type);
      projections.push_back(expr);
      auto fa = std::dynamic_pointer_cast<
          const facebook::velox::core::FieldAccessTypedExpr>(expr);
      names.push_back(
          (fa && !fa->name().empty()) ? fa->name()
                                      : "proj_" + std::to_string(i));
    }
  }

  return std::make_shared<facebook::velox::core::ProjectNode>(
      nextNodeId(), std::move(names), std::move(projections), current_input);
}

// ---------------------------------------------------------------------------
// ExtensionMultiRel → TableFunctionNode (table-valued functions)
//
// The Java planner encodes table function invocations as ExtensionMultiRel
// with a TableFunctionDetail payload in the detail field. We parse the
// function name, arguments, and output schema, then create a TableFunctionNode.
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertExtensionMultiRel(
    const substrait::ExtensionMultiRel& ext_rel,
    const std::vector<split::SplitConfig>& splits) {
  // Convert input sources (if any — lateral join pattern has one input).
  std::shared_ptr<const facebook::velox::core::PlanNode> input = nullptr;
  if (ext_rel.inputs_size() > 0) {
    input = convertRel(ext_rel.inputs(0), splits);
  }

  // Parse the TableFunctionDetail from the Any-packed detail field.
  if (!ext_rel.has_detail()) {
    throw std::runtime_error(
        "VeloxPlanConverter: ExtensionMultiRel missing detail payload "
        "for table function");
  }

  pangrova::native::protocol::v1::TableFunctionDetail detail;
  if (!ext_rel.detail().UnpackTo(&detail)) {
    throw std::runtime_error(
        "VeloxPlanConverter: failed to unpack TableFunctionDetail from "
        "ExtensionMultiRel detail field");
  }

  // Extract function name.
  const std::string& function_name = detail.function_name();
  if (function_name.empty()) {
    throw std::runtime_error(
        "VeloxPlanConverter: TableFunctionDetail has empty function_name");
  }

  // Validate that the function is registered.
  auto* registry =
      pangrova::jni::NativeEngineLifecycle::instance().tableFunctionRegistry();
  if (!registry || !registry->isRegistered(function_name)) {
    throw std::runtime_error(
        "Unsupported table function: " + function_name);
  }

  // Convert output schema.
  if (!detail.has_output_schema()) {
    throw std::runtime_error(
        "VeloxPlanConverter: TableFunctionDetail missing output_schema "
        "for function: " + function_name);
  }
  auto output_type = convertNamedStruct(detail.output_schema());

  // Convert argument expressions. Use the input type for field reference
  // resolution if an input is present, otherwise use an empty RowType.
  facebook::velox::RowTypePtr arg_context_type;
  if (input) {
    arg_context_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
        input->outputType());
  } else {
    arg_context_type = facebook::velox::ROW({}, {});
  }

  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> arguments;
  arguments.reserve(detail.arguments_size());
  for (int i = 0; i < detail.arguments_size(); ++i) {
    arguments.push_back(convertExpression(detail.arguments(i), arg_context_type));
  }

  // Create the TableFunctionNode.
  if (input) {
    // Lateral join: build combined output type = [input_cols..., func_cols...].
    auto input_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
        input->outputType());
    auto func_output_type =
        std::dynamic_pointer_cast<const facebook::velox::RowType>(output_type);

    std::vector<std::string> combined_names;
    std::vector<facebook::velox::TypePtr> combined_types;
    combined_names.reserve(input_type->size() + func_output_type->size());
    combined_types.reserve(input_type->size() + func_output_type->size());

    for (size_t i = 0; i < input_type->size(); ++i) {
      combined_names.push_back(input_type->nameOf(i));
      combined_types.push_back(input_type->childAt(i));
    }
    for (size_t i = 0; i < func_output_type->size(); ++i) {
      combined_names.push_back(func_output_type->nameOf(i));
      combined_types.push_back(func_output_type->childAt(i));
    }

    auto combined_output_type = facebook::velox::ROW(
        std::move(combined_names), std::move(combined_types));

    return std::make_shared<TableFunctionNode>(
        nextNodeId(), function_name, std::move(arguments),
        std::move(combined_output_type), std::move(input));
  }
  return std::make_shared<TableFunctionNode>(
      nextNodeId(), function_name, std::move(arguments),
      std::move(output_type));
}

// ---------------------------------------------------------------------------
// MergeJoinRel → MergeJoinNode (sort-merge join)
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertMergeJoinRel(
    const substrait::MergeJoinRel& merge_join_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto left = convertRel(merge_join_rel.left(), splits);
  auto right = convertRel(merge_join_rel.right(), splits);

  auto left_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      left->outputType());
  auto right_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      right->outputType());

  // Extract join keys from left and right field references.
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      left_keys;
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      right_keys;

  for (const auto& key : merge_join_rel.left_keys()) {
    auto expr = convertFieldReference(key, left_type);
    auto field = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(expr);
    if (field) left_keys.push_back(field);
  }
  for (const auto& key : merge_join_rel.right_keys()) {
    auto expr = convertFieldReference(key, right_type);
    auto field = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(expr);
    if (field) right_keys.push_back(field);
  }

  // Determine whether inputs are pre-sorted from the MergeJoinHint in
  // advanced_extension.enhancement. When not pre-sorted, wrap each input
  // with an OrderByNode on the join keys.
  bool inputs_pre_sorted = true;
  if (merge_join_rel.has_advanced_extension() &&
      merge_join_rel.advanced_extension().has_enhancement()) {
    pangrova::native::protocol::v1::MergeJoinHint hint;
    if (merge_join_rel.advanced_extension().enhancement().UnpackTo(&hint)) {
      inputs_pre_sorted = hint.inputs_pre_sorted();
    }
  }

  if (!inputs_pre_sorted) {
    // Wrap left input with OrderByNode on left join keys (ASC NULLS FIRST).
    std::vector<facebook::velox::core::SortOrder> sort_orders(
        left_keys.size(),
        facebook::velox::core::SortOrder(true, true));
    left = std::make_shared<facebook::velox::core::OrderByNode>(
        nextNodeId(), left_keys, sort_orders, /*isPartial=*/false, left);

    // Wrap right input with OrderByNode on right join keys (ASC NULLS FIRST).
    std::vector<facebook::velox::core::SortOrder> right_sort_orders(
        right_keys.size(),
        facebook::velox::core::SortOrder(true, true));
    right = std::make_shared<facebook::velox::core::OrderByNode>(
        nextNodeId(), right_keys, right_sort_orders, /*isPartial=*/false, right);
  }

  auto join_type = mapMergeJoinType(merge_join_rel.type());

  // Build output type with duplicate column name deduplication.
  std::vector<std::string> output_names;
  std::vector<facebook::velox::TypePtr> output_types;
  std::unordered_set<std::string> left_name_set;
  std::unordered_set<std::string> all_names;
  for (size_t i = 0; i < left_type->size(); ++i) {
    output_names.push_back(left_type->nameOf(i));
    output_types.push_back(left_type->childAt(i));
    left_name_set.insert(left_type->nameOf(i));
    all_names.insert(left_type->nameOf(i));
  }
  // Map from original right column name → deduplicated name for key renaming.
  std::unordered_map<std::string, std::string> right_rename_map;
  for (size_t i = 0; i < right_type->size(); ++i) {
    std::string orig = right_type->nameOf(i);
    std::string name = orig;
    if (all_names.count(name) > 0) {
      name = orig + "_right";
      int suffix = 2;
      while (all_names.count(name) > 0) {
        name = orig + "_right" + std::to_string(suffix++);
      }
    }
    right_rename_map[orig] = name;
    output_names.push_back(name);
    output_types.push_back(right_type->childAt(i));
    all_names.insert(name);
  }
  auto output_type =
      facebook::velox::ROW(std::move(output_names), std::move(output_types));

  // Convert post-join filter if present.
  std::shared_ptr<const facebook::velox::core::ITypedExpr> filter = nullptr;
  if (merge_join_rel.has_post_join_filter()) {
    filter = convertExpression(merge_join_rel.post_join_filter(), output_type);
  }

  // Velox MergeJoinNode validates that left and right input types have no
  // overlapping column names. Insert a ProjectNode to rename right side
  // when overlap exists (same pattern as HashJoinRel).
  bool hasOverlap = false;
  for (size_t i = 0; i < right_type->size() && !hasOverlap; ++i) {
    if (left_name_set.count(right_type->nameOf(i)) > 0) {
      hasOverlap = true;
    }
  }

  if (hasOverlap) {
    std::vector<std::string> renamed_names;
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> projections;
    renamed_names.reserve(right_type->size());
    projections.reserve(right_type->size());
    for (size_t i = 0; i < right_type->size(); ++i) {
      std::string orig_name = right_type->nameOf(i);
      renamed_names.push_back(right_rename_map[orig_name]);
      projections.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              right_type->childAt(i), orig_name));
    }
    auto rename_project = std::make_shared<facebook::velox::core::ProjectNode>(
        nextNodeId(), renamed_names, projections, right);

    // Rename right keys to match the projected names.
    std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
        renamed_right_keys;
    for (const auto& rk : right_keys) {
      std::string new_name = right_rename_map.count(rk->name())
          ? right_rename_map[rk->name()] : rk->name();
      renamed_right_keys.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              rk->type(), new_name));
    }

    return std::make_shared<facebook::velox::core::MergeJoinNode>(
        nextNodeId(), join_type, left_keys, renamed_right_keys,
        filter, left, rename_project, output_type);
  }

  return std::make_shared<facebook::velox::core::MergeJoinNode>(
      nextNodeId(), join_type, left_keys, right_keys,
      filter, left, right, output_type);
}

// ---------------------------------------------------------------------------
// CrossRel → NestedLoopJoinNode (cross product or nested-loop join with filter)
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertCrossRel(
    const substrait::CrossRel& cross_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto left = convertRel(cross_rel.left(), splits);
  auto right = convertRel(cross_rel.right(), splits);

  auto left_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      left->outputType());
  auto right_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      right->outputType());

  // Deduplicate column names when both sides share the same names.
  // Without this, Velox RowType validation fails on duplicate names.
  std::vector<std::string> output_names;
  std::vector<facebook::velox::TypePtr> output_types;
  std::unordered_set<std::string> left_name_set;
  std::unordered_set<std::string> all_names;
  for (size_t i = 0; i < left_type->size(); ++i) {
    output_names.push_back(left_type->nameOf(i));
    output_types.push_back(left_type->childAt(i));
    left_name_set.insert(left_type->nameOf(i));
    all_names.insert(left_type->nameOf(i));
  }
  // Map from original right column name → deduplicated name.
  std::unordered_map<std::string, std::string> right_rename_map;
  for (size_t i = 0; i < right_type->size(); ++i) {
    std::string orig = right_type->nameOf(i);
    std::string name = orig;
    if (all_names.count(name) > 0) {
      name = orig + "_1";
      int suffix = 2;
      while (all_names.count(name) > 0) {
        name = orig + "_" + std::to_string(suffix++);
      }
    }
    right_rename_map[orig] = name;
    output_names.push_back(name);
    output_types.push_back(right_type->childAt(i));
    all_names.insert(name);
  }
  auto output_type =
      facebook::velox::ROW(std::move(output_names), std::move(output_types));

  // Convert condition expression if present (non-equi join filter).
  std::shared_ptr<const facebook::velox::core::ITypedExpr> filter = nullptr;
  if (cross_rel.has_expression()) {
    filter = convertExpression(cross_rel.expression(), output_type);
  }

  // Map join type from CrossRel type field (defaults to INNER when unspecified).
  auto join_type = mapCrossRelJoinType(cross_rel.type());

  // Velox NestedLoopJoinNode validates that left and right input types have no
  // overlapping column names. Insert a ProjectNode to rename right side
  // when overlap exists (same pattern as HashJoinRel/MergeJoinRel).
  bool hasOverlap = false;
  for (size_t i = 0; i < right_type->size() && !hasOverlap; ++i) {
    if (left_name_set.count(right_type->nameOf(i)) > 0) {
      hasOverlap = true;
    }
  }

  if (hasOverlap) {
    std::vector<std::string> renamed_names;
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> projections;
    renamed_names.reserve(right_type->size());
    projections.reserve(right_type->size());
    for (size_t i = 0; i < right_type->size(); ++i) {
      std::string orig_name = right_type->nameOf(i);
      renamed_names.push_back(right_rename_map[orig_name]);
      projections.push_back(
          std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
              right_type->childAt(i), orig_name));
    }
    auto rename_project = std::make_shared<facebook::velox::core::ProjectNode>(
        nextNodeId(), renamed_names, projections, right);

    return std::make_shared<facebook::velox::core::NestedLoopJoinNode>(
        nextNodeId(),
        join_type,
        filter,
        left,
        rename_project,
        output_type);
  }

  return std::make_shared<facebook::velox::core::NestedLoopJoinNode>(
      nextNodeId(),
      join_type,
      filter,
      left,
      right,
      output_type);
}

}  // namespace pangrova::velox
