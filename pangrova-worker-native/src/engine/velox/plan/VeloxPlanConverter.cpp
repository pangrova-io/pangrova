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
#include "engine/plan/SubstraitPlanParser.h"
#include "engine/plan/SubstraitProtocolVersion.h"
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "capabilities/capability/FunctionCatalogPublication.h"
#include "capabilities/capability/FunctionPlanBindings.h"

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


// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

VeloxPlanConverter::VeloxPlanConverter(
    std::shared_ptr<const facebook::velox::exec::FunctionRegistrySnapshot> function_registry,
    std::shared_ptr<const capability::FunctionCatalogPublication> function_catalog)
    : function_registry_(std::move(function_registry)), function_catalog_(std::move(function_catalog)) {
  if (!function_registry_ || (function_catalog_ && function_catalog_->registry() != function_registry_)) {
    throw std::invalid_argument("Plan conversion requires its admitted function registry owner");
  }
}

void VeloxPlanConverter::requireFunctionKind(
    uint32_t anchor, pangrova::protocol::v2::FunctionKind kind) const {
  const auto found = function_declarations_.find(anchor);
  if (found == function_declarations_.end()) {
    throw std::runtime_error("Unbound canonical function anchor: " + std::to_string(anchor) +
        " kind=" + pangrova::protocol::v2::FunctionKind_Name(kind));
  }
  if (found->second.ref().kind() != kind) {
    throw std::runtime_error("Function reference does not match its canonical invocation kind");
  }
}

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convert(
    const std::vector<uint8_t>& plan_bytes,
    const std::vector<split::SplitConfig>& splits,
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
  if (!pangrova::plan::parseSubstraitPlan(
          plan_bytes.data(), plan_bytes.size(), &substrait_plan)) {
    throw std::runtime_error(
        "VeloxPlanConverter: failed to parse Substrait plan protobuf "
        "(plan_bytes.size=" + std::to_string(plan_bytes.size()) + ")");
  }

  pangrova::plan::validateSubstraitVersion(substrait_plan);

  return convertValidatedPlan(substrait_plan, splits, operator_bindings,
      std::move(artifact_build_context), retrieval_split_overrides, shuffle_partition_count);
}

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertValidatedPlan(
    const substrait::Plan& substrait_plan,
    const std::vector<split::SplitConfig>& splits,
    const std::unordered_map<
        std::string, pangrova::protocol::v2::OperatorBinding>& operator_bindings,
    std::shared_ptr<ArtifactBuildTaskContext> artifact_build_context,
    const std::unordered_map<
        std::string, std::vector<split::SplitConfig>>& retrieval_split_overrides,
    int32_t shuffle_partition_count) {
  single_match_join_requirements_.clear();
  if (substrait_plan.relations_size() == 0) {
    throw std::runtime_error("VeloxPlanConverter: Substrait plan contains no relations");
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
  relation_output_schemas_.clear();
  canonical_output_schema_.Clear();
  result_output_schema_.Clear();
  serde_schema_bindings_.clear();
  result_column_labels_.clear();
  function_map_.clear();
  function_declarations_ = capability::bindPlanFunctionDeclarations(substrait_plan);
  scan_node_output_types_.clear();
  retrieval_split_overrides_ = retrieval_split_overrides;
  scan_node_split_overrides_.clear();
  operator_bindings_ = operator_bindings;
  consumed_operator_bindings_.clear();
  artifact_build_context_ = std::move(artifact_build_context);

  for (const auto& [anchor, definition] : function_declarations_) {
    function_map_.emplace(anchor, definition.implementation_name());
  }

  // Extract the root relation.
  const auto& plan_rel = substrait_plan.relations(0);
  if (plan_rel.has_root()) {
    auto node = convertRel(plan_rel.root().input(), splits);
    canonical_output_schema_ = relation_output_schemas_.at(&plan_rel.root().input());
    if ((shuffle_partition_count_ > 0) != shuffle_output_spec_.has_value()) {
      throw std::runtime_error(
          "Shuffle task and producer ExchangeRel must be bound together");
    }
    bindResultColumnLabels(plan_rel.root(), node->outputType());
    validateAllOperatorBindingsConsumed();
    LOG(INFO) << "event=plan_conversion_complete"
              << " output_type=" << (node ? node->outputType()->toString() : "null")
              << " root_node=" << (node ? node->name() : "null");
    return node;
  }
  if (plan_rel.has_rel()) {
    auto node = convertRel(plan_rel.rel(), splits);
    canonical_output_schema_ = relation_output_schemas_.at(&plan_rel.rel());
    if ((shuffle_partition_count_ > 0) != shuffle_output_spec_.has_value()) {
      throw std::runtime_error(
          "Shuffle task and producer ExchangeRel must be bound together");
    }
    validateAllOperatorBindingsConsumed();
    LOG(INFO) << "event=plan_conversion_complete_rel"
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
