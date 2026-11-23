// VeloxPlanConverter: Converts Substrait plan bytes into a Velox PlanNode tree.
// Since Velox no longer ships a built-in Substrait converter (removed from
// main branch), this module implements the conversion directly using
// Substrait protobuf definitions and Velox PlanNode API.
//
// Supported operators: TableScan, Filter, Project, Aggregate,
// HashJoin, Exchange, Sort (OrderBy), Limit/TopN, Window,
// SetRel (UNION_ALL, INTERSECT, EXCEPT).
//
// Supported expressions: FieldReference, Literal (all types including null),
// ScalarFunction, Cast, IfThen (CASE/WHEN), WindowFunction.
//
// Unsupported Rel/Expression types throw std::runtime_error with type name.
//
// Placement: velox/ — Velox integration adapters.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "engine/split/SplitConfig.h"
#include "engine/velox/plan/ShuffleOutputSpec.h"
#include "engine/velox/runner/SingleMatchJoinRequirement.h"
#include "pangrova/protocol/v2/execution_binding.pb.h"
#include "pangrova/protocol/v2/capability.pb.h"
#include "substrait/algebra.pb.h"
#include "velox/core/PlanNode.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::exec { class FunctionRegistrySnapshot; }

// Forward-declare Substrait protobuf types to avoid header pollution.
namespace substrait {
class Rel;
class ReadRel;
class FilterRel;
class ProjectRel;
class AggregateRel;
class HashJoinRel;
class MergeJoinRel;
class JoinRel;
class NestedLoopJoinRel;
class ExchangeRel;
class SortRel;
class FetchRel;
class ConsistentPartitionWindowRel;
class SetRel;
class CrossRel;
class ExtensionSingleRel;
class ExtensionMultiRel;
class Expression;
class SortField;
class Type;
class NamedStruct;
}  // namespace substrait

namespace pangrova::protocol::v2 {
class RetrievalSourceDetail;
class RetrievalMergeDetail;
class RerankDetail;
class ArtifactBuildDetail;
class TableFunctionDetail;
}  // namespace pangrova::protocol::v2

namespace pangrova::velox {

struct ArtifactBuildTaskContext;

// Converts Substrait plan bytes to a Velox PlanNode tree.
// Throws std::runtime_error for unsupported Rel/Expression types.
// @NotThreadSafe — holds mutable state (next_node_id_, function_map_,
// scan_node_output_types_). Use one instance per task.
class VeloxPlanConverter {
 public:
  explicit VeloxPlanConverter(
      std::shared_ptr<const facebook::velox::exec::FunctionRegistrySnapshot> function_registry);
  // Convert raw Substrait Plan protobuf bytes into a Velox PlanNode.
  // splits: connector split metadata for table scan operators.
  // is_result_task: when true, wraps the plan root with a
  //   PartitionedOutputNode so that Velox serializes output via the
  //   specified serde and routes it through OutputBufferManager for native
  //   result serving.
  // result_serde_name: serde name for PartitionedOutputNode when
  //   is_result_task is true. The sole supported name is "ArrowBatch".
  std::shared_ptr<const facebook::velox::core::PlanNode> convert(
      const std::vector<uint8_t>& plan_bytes,
      const std::vector<split::SplitConfig>& splits,
      bool is_result_task = false,
      const std::string& result_serde_name = "ArrowBatch",
      const std::unordered_map<
          std::string, pangrova::protocol::v2::OperatorBinding>&
          operator_bindings = {},
      std::shared_ptr<ArtifactBuildTaskContext> artifact_build_context = nullptr,
      const std::unordered_map<
          std::string, std::vector<split::SplitConfig>>&
          retrieval_split_overrides = {},
      int32_t shuffle_partition_count = 0);

  const std::optional<ShuffleOutputSpec>& shuffleOutputSpec() const {
    return shuffle_output_spec_;
  }

  const std::vector<std::string>& resultColumnLabels() const {
    return result_column_labels_;
  }

  const substrait::NamedStruct& canonicalOutputSchema() const { return canonical_output_schema_; }
  const substrait::NamedStruct& resultOutputSchema() const { return result_output_schema_; }

  const std::unordered_map<std::string, substrait::NamedStruct>& serdeSchemaBindings() const {
    return serde_schema_bindings_;
  }

  /** Uses the execution schema converter for metadata-only storage commands. */
  facebook::velox::RowTypePtr convertStorageSchema(
      const substrait::NamedStruct& schema) {
    return convertNamedStruct(schema);
  }

  // Look up the output RowType for a TableScan node by its plan node id.
  // Returns the RowType if found, or std::nullopt if the id is unknown.
  // Used by DynamicFilterConverter to resolve column names to column indices.
  std::optional<facebook::velox::RowTypePtr> getScanNodeOutputType(
      const std::string& plan_node_id) const;

  // Return all scan node output types collected during plan conversion.
  // Used by PlanExecutionDriver to persist the mapping for dynamic filter
  // column resolution after the converter is destroyed.
  const std::unordered_map<std::string, facebook::velox::RowTypePtr>&
  scanNodeOutputTypes() const {
    return scan_node_output_types_;
  }

  /** Per-scan split filters produced by immutable retrieval artifacts. */
  const std::unordered_map<
      std::string, std::vector<split::SplitConfig>>&
  scanNodeSplitOverrides() const {
    return scan_node_split_overrides_;
  }

  // Standard Substrait LEFT_SINGLE joins lowered to Velox LEFT joins. The
  // executor validates their exact probe cardinality before task success.
  const std::vector<SingleMatchJoinRequirement>& singleMatchJoinRequirements() const {
    return single_match_join_requirements_;
  }

  const pangrova::protocol::v2::OperatorBinding& requireOperatorBinding(
      const std::string& plan_node_id,
      const pangrova::protocol::v2::CapabilityRef& capability);

  void validateAllOperatorBindingsConsumed() const;

 private:
  void requireFunctionKind(uint32_t anchor, pangrova::protocol::v2::FunctionKind kind) const;
  const std::shared_ptr<const facebook::velox::exec::FunctionRegistrySnapshot> function_registry_;
  // Monotonically increasing node id counter for unique PlanNode ids.
  int next_node_id_ = 0;
  int32_t shuffle_partition_count_ = 0;
  std::optional<ShuffleOutputSpec> shuffle_output_spec_;
  std::unordered_map<const substrait::Rel*, facebook::velox::RowTypePtr>
      relation_output_types_;
  std::unordered_map<std::string, facebook::velox::RowTypePtr> logical_node_types_;
  std::unordered_map<const substrait::Rel*, substrait::NamedStruct> relation_output_schemas_;
  substrait::NamedStruct canonical_output_schema_;
  substrait::NamedStruct result_output_schema_;
  std::unordered_map<std::string, substrait::NamedStruct> serde_schema_bindings_;
  std::vector<std::string> result_column_labels_;

  // Thread ownership tracking for @NotThreadSafe enforcement.
  // Set on first convert() call; subsequent calls assert same thread.
  // Debug-only: zero overhead in release builds.
  mutable std::thread::id owner_thread_id_;
  mutable bool owner_set_ = false;

  // Function reference → resolved function name map.
  // Built from Plan.extensions (SimpleExtensionDeclaration.ExtensionFunction)
  // and queried when converting scalar/aggregate function calls.
  std::unordered_map<uint32_t, std::string> function_map_;
  std::unordered_map<uint32_t, pangrova::protocol::v2::SubstraitFunctionDefinition> function_declarations_;

  // TableScan planNodeId → output RowType mapping.
  // Populated during convertReadRel() for each TableScanNode created.
  // Consumed by DynamicFilterConverter to resolve column name → column index.
  std::unordered_map<std::string, facebook::velox::RowTypePtr>
      scan_node_output_types_;
  std::unordered_map<std::string, std::vector<split::SplitConfig>>
      retrieval_split_overrides_;
  std::unordered_map<std::string, std::vector<split::SplitConfig>>
      scan_node_split_overrides_;
  std::vector<SingleMatchJoinRequirement> single_match_join_requirements_;

  std::unordered_map<std::string, pangrova::protocol::v2::OperatorBinding>
      operator_bindings_;
  std::unordered_set<std::string> consumed_operator_bindings_;
  std::shared_ptr<ArtifactBuildTaskContext> artifact_build_context_;

  // Sorting keys associated with RowNumberNodes created from window functions
  // that had ORDER BY clauses. RowNumberNode itself doesn't store sorting keys,
  // but TopNRowNumberNode needs them. This map is populated during window
  // conversion and consumed by convertFilterRel for TopN pattern detection.
  // Key: RowNumberNode plan node ID. Value: (sorting_keys, sorting_orders).
  struct RowNumberSortInfo {
    std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
        sorting_keys;
    std::vector<facebook::velox::core::SortOrder> sorting_orders;
  };
  std::unordered_map<std::string, RowNumberSortInfo>
      row_number_sort_info_;

  // Wrap a plan node with PartitionedOutputNode for result tasks.
  // No-op when is_result_task is false or node is already a PartitionedOutputNode.
  // serde_name: the canonical serialization format name ("ArrowBatch").
  std::shared_ptr<const facebook::velox::core::PlanNode> wrapResultTaskOutput(
      std::shared_ptr<const facebook::velox::core::PlanNode> node,
      bool is_result_task,
      const std::string& serde_name);

  // Generate a unique plan node id string.
  std::string nextNodeId();

  // Recursive dispatcher: converts a Substrait Rel to a Velox PlanNode.
  std::shared_ptr<const facebook::velox::core::PlanNode> convertRel(
      const substrait::Rel& rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertDirectRel(
      const substrait::Rel& rel,
      const std::vector<split::SplitConfig>& splits);
  facebook::velox::RowTypePtr directLogicalType(
      const substrait::Rel& rel,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& node);
  const facebook::velox::RowTypePtr& logicalNodeType(
      const std::shared_ptr<const facebook::velox::core::PlanNode>& node) const;
  void bindResultColumnLabels(const substrait::RelRoot& root,
                             const facebook::velox::RowTypePtr& type);

  // Operator-specific converters.
  std::shared_ptr<const facebook::velox::core::PlanNode> convertReadProjection(
      const substrait::Expression::MaskExpression& mask,
      std::shared_ptr<const facebook::velox::core::PlanNode> input);

  facebook::velox::core::TypedExprPtr convertMaskSelection(
      const substrait::Expression::MaskExpression::Select& selection,
      facebook::velox::core::TypedExprPtr value, bool maintain);
  facebook::velox::core::TypedExprPtr convertReferenceSegment(
      const substrait::Expression::ReferenceSegment& segment,
      facebook::velox::core::TypedExprPtr value);
  facebook::velox::core::TypedExprPtr convertStructMask(
      const substrait::Expression::MaskExpression::StructSelect& selection,
      facebook::velox::core::TypedExprPtr value, bool maintain);
  facebook::velox::core::TypedExprPtr convertListMask(
      const substrait::Expression::MaskExpression::ListSelect& selection,
      facebook::velox::core::TypedExprPtr value, bool maintain);
  facebook::velox::core::TypedExprPtr convertMapMask(
      const substrait::Expression::MaskExpression::MapSelect& selection,
      facebook::velox::core::TypedExprPtr value, bool maintain);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertReadRel(
      const substrait::ReadRel& read_rel,
      const std::vector<split::SplitConfig>& splits);

  // Convert a Substrait VirtualTable (inline literal data) to a Velox ValuesNode.
  std::shared_ptr<const facebook::velox::core::PlanNode> convertVirtualTable(
      const substrait::ReadRel& read_rel,
      const facebook::velox::RowTypePtr& output_type);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertFilterRel(
      const substrait::FilterRel& filter_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertProjectRel(
      const substrait::ProjectRel& project_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertAggregateRel(
      const substrait::AggregateRel& aggregate_rel,
      const std::vector<split::SplitConfig>& splits);

  // Multi-grouping-set expansion (GROUPING SETS / CUBE / ROLLUP).
  // Builds GroupIdNode (row replication + NULL fill + groupId assignment)
  // followed by aggregation and a projection retaining canonical set identity.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertMultiGroupingAggregate(
      const substrait::AggregateRel& aggregate_rel,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);
  std::shared_ptr<const facebook::velox::core::PlanNode> projectGroupingOutput(
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      size_t groupingKeyCount, size_t measureCount);

  // ExtensionSingleRel → typed Pangrova relation or UnnestNode.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertExtensionSingleRel(
      const substrait::ExtensionSingleRel& ext_rel,
      const std::vector<split::SplitConfig>& splits);

  // ExtensionLeafRel → typed native source such as a distributed exchange.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertExtensionLeafRel(const substrait::ExtensionLeafRel& ext_rel);

  // Typed exact Top-K retrieval source.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertTopKRetrieval(
      const pangrova::protocol::v2::RetrievalSourceDetail& detail,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type,
      const std::vector<split::SplitConfig>& splits);

  facebook::velox::core::PlanNodePtr convertFilteredRetrievalInput(
      const pangrova::protocol::v2::RetrievalSourceDetail& detail,
      const pangrova::protocol::v2::OperatorBinding& binding,
      facebook::velox::core::PlanNodePtr input,
      const std::vector<split::SplitConfig>& splits);

  // Typed, bounded Native RRF / weighted-linear global candidate fusion.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertRetrievalMerge(
      const pangrova::protocol::v2::RetrievalMergeDetail& detail,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);

  // Bounded cross-encoder scoring over a complete Native candidate stream.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertRerank(
      const pangrova::protocol::v2::RerankDetail& detail,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);

  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertArtifactBuild(
      const pangrova::protocol::v2::ArtifactBuildDetail& detail,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);

  // Exact generated-value sidecar join expressed through the canonical generic
  // table-function envelope on an ExtensionSingleRel.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertGeneratedValuesOverlay(
      const pangrova::protocol::v2::TableFunctionDetail& detail,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertUnnest(
      const pangrova::protocol::v2::TableFunctionDetail& detail,
      std::shared_ptr<const facebook::velox::core::PlanNode> input);

  // ExtensionLeafRel for source functions; ExtensionSingleRel for lateral input.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertTableFunctionDetail(
      const pangrova::protocol::v2::TableFunctionDetail& detail);

  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertExtensionMultiRel(
      const substrait::ExtensionMultiRel& ext_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertHashJoinRel(
      const substrait::HashJoinRel& hash_join_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertJoinRel(
      const substrait::JoinRel& join_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertNestedLoopJoinRel(
      const substrait::NestedLoopJoinRel& join_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertExchangeRel(
      const substrait::ExchangeRel& exchange_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertSortRel(
      const substrait::SortRel& sort_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertFetchRel(
      const substrait::FetchRel& fetch_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertWindowRel(
      const substrait::ConsistentPartitionWindowRel& window_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertMergeJoinRel(
      const substrait::MergeJoinRel& merge_join_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertCrossRel(
      const substrait::CrossRel& cross_rel,
      const std::vector<split::SplitConfig>& splits);

  std::shared_ptr<const facebook::velox::core::PlanNode> convertSetRel(
      const substrait::SetRel& set_rel,
      const std::vector<split::SplitConfig>& splits);

  // Convert a ProjectRel containing window function expressions into a
  // WindowNode + ProjectNode pipeline.  Called from convertProjectRel when
  // the Java planner encodes window functions as WindowFunctionInvocation
  // expressions inside a ProjectRel (Substrait Java SDK v0.23.0 lacks a
  // relation-level WindowRel builder).
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertProjectWithWindowFunctions(
      const substrait::ProjectRel& project_rel,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);

  // --- AggregateRel helpers ---

  struct PreparedAggregate {
    std::shared_ptr<const facebook::velox::core::PlanNode> input;
    std::vector<std::string> names;
    std::vector<facebook::velox::core::AggregationNode::Aggregate> measures;
    std::vector<facebook::velox::core::FieldAccessTypedExprPtr> keys;
    std::vector<facebook::velox::TypePtr> intermediate_types;
    facebook::velox::core::AggregationNode::Step step{};
    bool requires_single = false;

    bool usesLocalPartial() const {
      return step == facebook::velox::core::AggregationNode::Step::kSingle && !requires_single;
    }
  };

  struct BoundAggregateMeasure {
    facebook::velox::core::AggregationNode::Aggregate measure;
    facebook::velox::TypePtr intermediate_type;
  };

  // Bind grouping keys in declaration order and prepare only computed aggregate inputs.
  PreparedAggregate prepareAggregate(
      const substrait::AggregateRel& aggregate_rel,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);

  facebook::velox::core::TypedExprPtr aggregateInputValue(
      const substrait::Expression& expression,
      const facebook::velox::RowTypePtr& input_type,
      std::vector<std::string>& names,
      std::vector<facebook::velox::core::TypedExprPtr>& projections,
      bool preserve_constant);

  BoundAggregateMeasure convertAggregateMeasure(
      const substrait::AggregateRel::Measure& measure,
      const facebook::velox::RowTypePtr& input_type,
      std::vector<std::string>& names,
      std::vector<facebook::velox::core::TypedExprPtr>& projections);

  // Detect pre-grouped keys when input is sorted on grouping keys.
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
  detectPreGroupedKeys(
      const std::shared_ptr<const facebook::velox::core::PlanNode>& input,
      const std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& grouping_keys);

  // Insert a LocalPartitionNode to colocate data by partition keys.
  // Uses kRepartition(hash) when keys are present, gather() otherwise.
  // Required before operators that assume per-key data colocation
  // (MarkDistinct, RowNumber) when max_drivers > 1.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  insertLocalPartitionExchange(
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const std::vector<std::shared_ptr<
          const facebook::velox::core::FieldAccessTypedExpr>>& partition_keys);

  // Split kSingle aggregation into kPartial→exchange→kFinal pipeline.
  std::shared_ptr<const facebook::velox::core::PlanNode> buildParallelAggregation(
      const std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& grouping_keys,
      const std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& pre_grouped_keys,
      const PreparedAggregate& aggregate,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const std::vector<facebook::velox::vector_size_t>& globalGroupingSets = {},
      const std::optional<facebook::velox::core::FieldAccessTypedExprPtr>& groupId = std::nullopt);

  // --- JoinRel helpers ---

  std::shared_ptr<const facebook::velox::core::PlanNode> rebaseJoinInput(
      std::shared_ptr<const facebook::velox::core::PlanNode> input, size_t offset);

  std::shared_ptr<const facebook::velox::core::PlanNode> applyPostJoinFilter(
      const substrait::Expression& predicate,
      std::shared_ptr<const facebook::velox::core::PlanNode> input);

  std::shared_ptr<const facebook::velox::core::PlanNode> finishJoinMarker(
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& outputType, bool anti);

  std::shared_ptr<const facebook::velox::core::PlanNode> buildNestedJoin(
      const std::string& kind,
      std::shared_ptr<const facebook::velox::core::PlanNode> left,
      std::shared_ptr<const facebook::velox::core::PlanNode> right,
      const std::shared_ptr<const facebook::velox::core::ITypedExpr>& condition);

  struct PreparedJoinKeys {
    std::shared_ptr<const facebook::velox::core::PlanNode> left;
    std::shared_ptr<const facebook::velox::core::PlanNode> right;
    std::vector<facebook::velox::core::FieldAccessTypedExprPtr> leftKeys;
    std::vector<facebook::velox::core::FieldAccessTypedExprPtr> rightKeys;
    facebook::velox::core::TypedExprPtr condition;
    bool hashable = true;
    bool ordinaryEquality = true;
  };

  PreparedJoinKeys prepareJoinKeys(
      const google::protobuf::RepeatedPtrField<substrait::ComparisonJoinKey>& keys,
      std::shared_ptr<const facebook::velox::core::PlanNode> left,
      std::shared_ptr<const facebook::velox::core::PlanNode> right);

  facebook::velox::core::TypedExprPtr convertJoinComparison(
      const substrait::ComparisonJoinKey::ComparisonType& comparison,
      facebook::velox::core::TypedExprPtr left,
      facebook::velox::core::TypedExprPtr right);

  std::shared_ptr<const facebook::velox::core::PlanNode> appendJoinKeys(
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const std::vector<facebook::velox::core::TypedExprPtr>& expressions,
      size_t offset,
      std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& keys);

  std::shared_ptr<const facebook::velox::core::PlanNode> buildKeyedJoin(
      const std::string& kind, PreparedJoinKeys keys,
      const facebook::velox::RowTypePtr& outputType,
      bool buildLeft, bool merge);

  // Extract equi-join key pairs and residual conditions from a join expression.
  void extractJoinKeys(
      const substrait::Expression& expr,
      const facebook::velox::RowTypePtr& left_type,
      const facebook::velox::RowTypePtr& right_type,
      const facebook::velox::RowTypePtr& output_type,
      int left_field_count,
      std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& left_keys,
      std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& right_keys);

  // --- SetRel helpers ---

  // Build a UnionNode (gather) for UNION ALL.
  std::shared_ptr<const facebook::velox::core::PlanNode> buildUnionAll(
      std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>> sources);

  // Build chained semi/anti joins for INTERSECT or EXCEPT.
  std::shared_ptr<const facebook::velox::core::PlanNode> buildIntersectOrExcept(
      std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>> sources,
      facebook::velox::core::JoinType join_type);

  std::shared_ptr<const facebook::velox::core::PlanNode> deduplicateSet(
      std::shared_ptr<const facebook::velox::core::PlanNode> input);

  // --- Window helpers (for convertProjectWithWindowFunctions) ---

  // Information about a single window function in a ProjectRel.
  struct WindowFuncInfo {
    int proj_index;
    const substrait::Expression_WindowFunction* wf;
  };

  // A group of window functions sharing the same partition/sort keys.
  struct WindowGroup {
    std::string signature;
    std::vector<WindowFuncInfo> funcs;
  };

  struct PreparedWindow {
    std::shared_ptr<const facebook::velox::core::PlanNode> input;
    std::vector<facebook::velox::core::FieldAccessTypedExprPtr> partitions;
    std::vector<facebook::velox::core::FieldAccessTypedExprPtr> sorts;
    std::vector<facebook::velox::core::SortOrder> orders;
    std::vector<std::string> names;
    std::vector<facebook::velox::core::WindowNode::Function> functions;
  };

  PreparedWindow prepareWindowGroup(
      const WindowGroup& group,
      const facebook::velox::RowTypePtr& expressionType,
      std::shared_ptr<const facebook::velox::core::PlanNode> input);

  facebook::velox::core::WindowNode::Function convertWindowFunction(
      const substrait::Expression_WindowFunction& function,
      const facebook::velox::RowTypePtr& expressionType,
      std::vector<std::string>& names,
      std::vector<facebook::velox::core::TypedExprPtr>& projections,
      const std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& sorts,
      const std::vector<facebook::velox::core::SortOrder>& orders);

  // Scan ProjectRel expressions for window function invocations.
  std::vector<WindowFuncInfo> extractWindowFunctions(
      const substrait::ProjectRel& project_rel);

  // Group window functions by partition/order key signature.
  std::vector<WindowGroup> groupWindowFunctions(
      const std::vector<WindowFuncInfo>& window_funcs);

  // Build a single WindowNode from a group of window functions.
  std::shared_ptr<const facebook::velox::core::PlanNode> buildWindowNode(
      const WindowGroup& group,
      const facebook::velox::RowTypePtr& current_type,
      std::shared_ptr<const facebook::velox::core::PlanNode> current_input,
      std::unordered_map<int, std::string>& proj_index_to_window_col);

  // Convert Substrait NamedStruct to Velox RowType.
  facebook::velox::RowTypePtr convertNamedStruct(
      const substrait::NamedStruct& named_struct);

  // Convert a single Substrait Type to a Velox Type.
  facebook::velox::TypePtr convertType(const substrait::Type& type);

  // Convert a Substrait Expression to a Velox ITypedExpr.
  std::shared_ptr<const facebook::velox::core::ITypedExpr> convertExpression(
      const substrait::Expression& expr,
      const facebook::velox::RowTypePtr& input_type);

  // Convert Substrait FieldReference to a Velox FieldAccessTypedExpr.
  std::shared_ptr<const facebook::velox::core::ITypedExpr> convertFieldReference(
      const substrait::Expression_FieldReference& field_ref,
      const facebook::velox::RowTypePtr& input_type);

  // Convert a Substrait SortField to Velox SortOrder.
  facebook::velox::core::SortOrder convertSortOrder(
      const substrait::SortField& sort_field);

  // One typed decoder is shared by scalar, collection, and Values literals.
  std::shared_ptr<const facebook::velox::core::ConstantTypedExpr> convertLiteral(
      const substrait::Expression_Literal& literal);
  std::shared_ptr<const facebook::velox::core::ConstantTypedExpr>
  convertDecimalLiteral(const substrait::Expression_Literal_Decimal& decimal);
  std::shared_ptr<const facebook::velox::core::ConstantTypedExpr>
  convertListLiteral(const substrait::Expression_Literal_List& list);
  std::shared_ptr<const facebook::velox::core::ConstantTypedExpr>
  convertMapLiteral(const substrait::Expression_Literal_Map& map);
  std::shared_ptr<const facebook::velox::core::ConstantTypedExpr>
  arrayConstant(facebook::velox::VectorPtr elements);
  std::shared_ptr<facebook::velox::memory::MemoryPool> literalPool();
  std::shared_ptr<const facebook::velox::core::ConstantTypedExpr> convertStructLiteral(
      const substrait::Expression_Literal_Struct& value);
  static facebook::velox::TypePtr convertDecimalType(int32_t precision, int32_t scale);
  static facebook::velox::TypePtr convertTimeType(int32_t precision);

  // Bind against this converter's immutable owner; only explicit casts coerce.
  std::shared_ptr<const facebook::velox::core::ITypedExpr>
  checkedScalarCall(
      const std::string& name,
      const facebook::velox::TypePtr& output_type,
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> args);
  std::shared_ptr<const facebook::velox::core::ITypedExpr> convertScalarFunction(
      const substrait::Expression_ScalarFunction& function,
      const facebook::velox::RowTypePtr& input_type);
  void appendGovernedAiArguments(
      const std::string& name,
      const substrait::Expression_ScalarFunction& function,
      const facebook::velox::TypePtr& output_type,
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& args);
  std::shared_ptr<const facebook::velox::core::ITypedExpr> convertNestedExpression(
      const substrait::Expression::Nested& expression,
      const facebook::velox::RowTypePtr& inputType);

  std::shared_ptr<const facebook::velox::core::ITypedExpr> convertIfThen(
      const substrait::Expression_IfThen& expression,
      const facebook::velox::RowTypePtr& input_type);
  std::shared_ptr<const facebook::velox::core::ITypedExpr> convertWindowFunction(
      const substrait::Expression_WindowFunction& function,
      const facebook::velox::RowTypePtr& input_type);
};

}  // namespace pangrova::velox
