// VeloxPlanConverter: Substrait → Velox PlanNode conversion entry point.
//
// This file contains the public API (convert, getScanNodeOutputType),
// node ID generation, recursive Rel dispatcher, and Substrait function
// name resolution. Type/expression conversion is in SubstraitTypeConversion.cpp.
// Operator-specific converters are in SubstraitRelConversion.cpp.
//
// Velox removed built-in Substrait support from main branch.
// This converter parses Substrait Plan protobuf and builds Velox PlanNode
// tree using Velox's core PlanNode API.
//
// Supported operators: TableScan, Filter, Project, Aggregate, HashJoin,
// Exchange, Sort (OrderBy), Limit/TopN, Window, SetRel (UNION_ALL,
// INTERSECT, EXCEPT). Recursive tree conversion handles nested operator
// chains. Unsupported Rel/Expression types throw std::runtime_error with
// type name for diagnostics.
#include "velox/plan/VeloxPlanConverter.h"

#include <cassert>
#include <stdexcept>
#include <unordered_map>

#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"

// Generated Substrait proto headers (from pangrova-worker-native/proto/).
#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"
#include "substrait/extensions/extensions.pb.h"

namespace pangrova::velox {

namespace {

// Extract the short function name from a Substrait extension function name.
// Substrait names are like "add:i32_i32", "lt:fp64_fp64", "sum:i64" etc.
// We strip everything after the first ':' to get just "add", "lt", "sum".
std::string extractShortFunctionName(const std::string& substrait_name) {
  auto colon_pos = substrait_name.find(':');
  if (colon_pos != std::string::npos) {
    return substrait_name.substr(0, colon_pos);
  }
  return substrait_name;
}

// Substrait → Velox function name mapping.
// Substrait uses standard function names from the specification;
// Velox uses Presto-compatible names. This table bridges the gap.
const std::unordered_map<std::string, std::string>& substraitToVeloxFunctionMap() {
  static const std::unordered_map<std::string, std::string> kMap = {
      // Arithmetic
      {"add", "plus"},
      {"subtract", "minus"},
      {"multiply", "multiply"},
      {"divide", "divide"},
      {"modulus", "mod"},
      {"negate", "negate"},
      {"abs", "abs"},
      {"power", "power"},
      {"sqrt", "sqrt"},
      {"round", "round"},
      {"ceil", "ceil"},
      {"floor", "floor"},
      // Comparison
      {"equal", "eq"},
      {"not_equal", "neq"},
      {"lt", "lt"},
      {"gt", "gt"},
      {"lte", "lte"},
      {"gte", "gte"},
      {"is_not_null", "is_not_null"},
      {"is_null", "is_null"},
      {"is_not_distinct_from", "is_not_distinct_from"},
      {"between", "between"},
      // Boolean
      {"and", "and"},
      {"or", "or"},
      {"not", "not"},
      // String
      {"concat", "concat"},
      {"char_length", "length"},
      {"upper", "upper"},
      {"lower", "lower"},
      {"substring", "substr"},
      {"like", "like"},
      {"trim", "trim"},
      {"ltrim", "ltrim"},
      {"rtrim", "rtrim"},
      {"replace", "replace"},
      {"starts_with", "starts_with"},
      // Type coercion
      {"cast", "cast"},
      // Date/time
      {"extract", "extract"},
      {"year", "year"},
      {"month", "month"},
      {"day", "day"},
      {"hour", "hour"},
      {"minute", "minute"},
      {"second", "second"},
      // Aggregate (Velox uses same names)
      {"sum", "sum"},
      {"count", "count"},
      {"min", "min"},
      {"max", "max"},
      {"avg", "avg"},
      {"count_star", "count"},
      // Substrait "collect" → Velox "array_agg"
      {"collect", "array_agg"},
      // Window
      {"row_number", "row_number"},
      {"rank", "rank"},
      {"dense_rank", "dense_rank"},
      {"ntile", "ntile"},
      {"lag", "lag"},
      {"lead", "lead"},
      {"first_value", "first_value"},
      {"last_value", "last_value"},
      // Coalesce / conditional
      {"coalesce", "coalesce"},
      {"if_then", "if"},
  };
  return kMap;
}

// Resolve a Substrait function name to its Velox equivalent.
std::string resolveVeloxFunctionName(const std::string& substrait_name) {
  const auto& map = substraitToVeloxFunctionMap();
  auto it = map.find(substrait_name);
  if (it != map.end()) {
    return it->second;
  }
  return substrait_name;
}

}  // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convert(
    const std::vector<uint8_t>& plan_bytes,
    const std::vector<split::SplitConfig>& splits,
    bool is_result_task,
    const std::string& result_serde_name,
    const std::unordered_map<
        std::string, pangrova::protocol::v2::OperatorBinding>&
        operator_bindings,
    std::shared_ptr<ArtifactBuildTaskContext> artifact_build_context,
    const std::unordered_map<
        std::string, std::vector<split::SplitConfig>>&
        retrieval_split_overrides,
    int32_t shuffle_partition_count) {
  single_match_join_requirements_.clear();
  if (plan_bytes.empty()) {
    return nullptr;
  }

  ::substrait::Plan substrait_plan;
  if (!substrait_plan.ParseFromArray(
          plan_bytes.data(), static_cast<int>(plan_bytes.size()))) {
    throw std::runtime_error(
        "VeloxPlanConverter: failed to parse Substrait plan protobuf "
        "(plan_bytes.size=" + std::to_string(plan_bytes.size()) + ")");
  }

  if (substrait_plan.relations_size() == 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: Substrait plan contains no relations "
        "(plan_bytes.size=" + std::to_string(plan_bytes.size()) + ")");
  }

  // Reset state for each conversion.
  // Debug-only thread ownership check: catch accidental cross-thread reuse.
  assert((!owner_set_ || owner_thread_id_ == std::this_thread::get_id()) &&
         "VeloxPlanConverter must not be shared across threads");
  owner_thread_id_ = std::this_thread::get_id();
  owner_set_ = true;

  next_node_id_ = 0;
  shuffle_partition_count_ = shuffle_partition_count;
  shuffle_output_spec_.reset();
  relation_output_types_.clear();
  logical_node_types_.clear();
  result_column_labels_.clear();
  function_map_.clear();
  scan_node_output_types_.clear();
  retrieval_split_overrides_ = retrieval_split_overrides;
  scan_node_split_overrides_.clear();
  operator_bindings_ = operator_bindings;
  consumed_operator_bindings_.clear();
  artifact_build_context_ = std::move(artifact_build_context);

  // Build function_reference → function_name map from plan extensions.
  for (int i = 0; i < substrait_plan.extensions_size(); ++i) {
    const auto& ext = substrait_plan.extensions(i);
    if (ext.has_extension_function()) {
      uint32_t anchor = ext.extension_function().function_anchor();
      std::string raw_name = ext.extension_function().name();
      std::string short_name = extractShortFunctionName(raw_name);
      std::string velox_name = resolveVeloxFunctionName(short_name);
      function_map_[anchor] = velox_name;
    }
  }

  // Extract the root relation.
  const auto& plan_rel = substrait_plan.relations(0);
  if (plan_rel.has_root()) {
    auto node = convertRel(plan_rel.root().input(), splits);
    if ((shuffle_partition_count_ > 0) != shuffle_output_spec_.has_value()) {
      throw std::runtime_error(
          "Shuffle task and producer ExchangeRel must be bound together");
    }
    bindResultColumnLabels(plan_rel.root(), node->outputType());
    // For result tasks, wrap the plan root with PartitionedOutputNode so that
    // Velox serializes output via PrestoVectorSerde inside the operator pipeline
    // and routes it through OutputBufferManager for native result serving.
    // Skip wrapping if the root is already a PartitionedOutputNode (e.g., from
    // an ExchangeRel that was converted to a partitioned output).
    node = wrapResultTaskOutput(node, is_result_task, result_serde_name);
    validateAllOperatorBindingsConsumed();
    LOG(INFO) << "event=plan_conversion_complete"
              << " is_result_task=" << is_result_task
              << " output_type=" << (node ? node->outputType()->toString() : "null")
              << " root_node=" << (node ? node->name() : "null");
    return node;
  }
  if (plan_rel.has_rel()) {
    auto node = convertRel(plan_rel.rel(), splits);
    if ((shuffle_partition_count_ > 0) != shuffle_output_spec_.has_value()) {
      throw std::runtime_error(
          "Shuffle task and producer ExchangeRel must be bound together");
    }
    node = wrapResultTaskOutput(node, is_result_task, result_serde_name);
    validateAllOperatorBindingsConsumed();
    LOG(INFO) << "event=plan_conversion_complete_rel"
              << " is_result_task=" << is_result_task
              << " output_type=" << (node ? node->outputType()->toString() : "null")
              << " root_node=" << (node ? node->name() : "null");
    return node;
  }
  return nullptr;
}

const pangrova::protocol::v2::OperatorBinding&
VeloxPlanConverter::requireOperatorBinding(
    const std::string& plan_node_id,
    const pangrova::protocol::v2::CapabilityRef& capability) {
  auto found = operator_bindings_.find(plan_node_id);
  if (found == operator_bindings_.end()) {
    throw std::runtime_error(
        "VeloxPlanConverter: missing OperatorBinding for plan node " +
        plan_node_id);
  }
  const auto& bound = found->second.capability();
  if (bound.capability_id() != capability.capability_id() ||
      bound.semantic_version() != capability.semantic_version() ||
      bound.signature_sha256() != capability.signature_sha256() ||
      (!capability.implementation_build_id().empty() &&
       bound.implementation_build_id() != capability.implementation_build_id())) {
    throw std::runtime_error(
        "VeloxPlanConverter: OperatorBinding capability differs from plan node " +
        plan_node_id);
  }
  if (!consumed_operator_bindings_.emplace(plan_node_id).second) {
    throw std::runtime_error(
        "VeloxPlanConverter: OperatorBinding is referenced more than once: " +
        plan_node_id);
  }
  return found->second;
}

void VeloxPlanConverter::validateAllOperatorBindingsConsumed() const {
  if (consumed_operator_bindings_.size() == operator_bindings_.size()) {
    return;
  }
  for (const auto& [plan_node_id, binding] : operator_bindings_) {
    (void)binding;
    if (!consumed_operator_bindings_.contains(plan_node_id)) {
      throw std::runtime_error(
          "VeloxPlanConverter: unused OperatorBinding for plan node " +
          plan_node_id);
    }
  }
}

// ---------------------------------------------------------------------------
// Scan node output type lookup
// ---------------------------------------------------------------------------

std::optional<facebook::velox::RowTypePtr>
VeloxPlanConverter::getScanNodeOutputType(
    const std::string& plan_node_id) const {
  auto it = scan_node_output_types_.find(plan_node_id);
  if (it != scan_node_output_types_.end()) {
    return it->second;
  }
  return std::nullopt;
}

// ---------------------------------------------------------------------------
// Node ID generation
// ---------------------------------------------------------------------------

std::string VeloxPlanConverter::nextNodeId() {
  return "node_" + std::to_string(next_node_id_++);
}

// ---------------------------------------------------------------------------
// Result task PartitionedOutputNode wrapping
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::wrapResultTaskOutput(
    std::shared_ptr<const facebook::velox::core::PlanNode> node,
    bool is_result_task,
    const std::string& serde_name) {
  if (!is_result_task || !node) {
    return node;
  }
  // Skip wrapping if the root is already a PartitionedOutputNode.
  if (std::dynamic_pointer_cast<
          const facebook::velox::core::PartitionedOutputNode>(node) !=
      nullptr) {
    return node;
  }
  auto result_output_type =
      std::dynamic_pointer_cast<const facebook::velox::RowType>(
          node->outputType());
  return std::make_shared<facebook::velox::core::PartitionedOutputNode>(
      nextNodeId(),
      facebook::velox::core::PartitionedOutputNode::Kind::kArbitrary,
      /*keys=*/std::vector<std::shared_ptr<
          const facebook::velox::core::ITypedExpr>>{},
      /*numPartitions=*/1,
      /*replicateNullsAndAny=*/false,
      /*partitionFunctionSpec=*/nullptr,
      result_output_type,
      serde_name,
      node);
}

// ---------------------------------------------------------------------------
// Recursive Rel dispatcher
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertDirectRel(
    const substrait::Rel& rel,
    const std::vector<split::SplitConfig>& splits) {
  switch (rel.rel_type_case()) {
    case substrait::Rel::kRead:
      return convertReadRel(rel.read(), splits);
    case substrait::Rel::kFilter:
      return convertFilterRel(rel.filter(), splits);
    case substrait::Rel::kProject:
      return convertProjectRel(rel.project(), splits);
    case substrait::Rel::kAggregate:
      return convertAggregateRel(rel.aggregate(), splits);
    case substrait::Rel::kHashJoin:
      return convertHashJoinRel(rel.hash_join(), splits);
    case substrait::Rel::kMergeJoin:
      return convertMergeJoinRel(rel.merge_join(), splits);
    case substrait::Rel::kJoin:
      return convertJoinRel(rel.join(), splits);
    case substrait::Rel::kNestedLoopJoin:
      return convertNestedLoopJoinRel(rel.nested_loop_join(), splits);
    case substrait::Rel::kExchange:
      return convertExchangeRel(rel.exchange(), splits);
    case substrait::Rel::kSort:
      return convertSortRel(rel.sort(), splits);
    case substrait::Rel::kFetch:
      return convertFetchRel(rel.fetch(), splits);
    case substrait::Rel::kWindow:
      return convertWindowRel(rel.window(), splits);
    case substrait::Rel::kSet:
      return convertSetRel(rel.set(), splits);
    case substrait::Rel::kCross:
      return convertCrossRel(rel.cross(), splits);
    case substrait::Rel::kExtensionLeaf:
      return convertExtensionLeafRel(rel.extension_leaf());
    case substrait::Rel::kExtensionSingle:
      return convertExtensionSingleRel(rel.extension_single(), splits);
    case substrait::Rel::kExtensionMulti:
      return convertExtensionMultiRel(rel.extension_multi(), splits);
    default:
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported Substrait rel type: " +
          std::to_string(static_cast<int>(rel.rel_type_case())));
  }
}

}  // namespace pangrova::velox
