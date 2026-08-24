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

#include "split/SplitConfig.h"
#include "substrait/algebra.pb.h"
#include "velox/core/PlanNode.h"
#include "velox/common/memory/Memory.h"

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

namespace pangrova::velox {

// Converts Substrait plan bytes to a Velox PlanNode tree.
// Throws std::runtime_error for unsupported Rel/Expression types.
// @NotThreadSafe — holds mutable state (next_node_id_, function_map_,
// scan_node_output_types_, values_pool_holders_). Use one instance per task.
class VeloxPlanConverter {
 public:
  // Convert raw Substrait Plan protobuf bytes into a Velox PlanNode.
  // splits: connector split metadata for table scan operators.
  // is_result_task: when true, wraps the plan root with a
  //   PartitionedOutputNode so that Velox serializes output via the
  //   specified serde and routes it through OutputBufferManager for native
  //   result serving.
  // result_serde_name: serde name for PartitionedOutputNode when
  //   is_result_task is true. "Presto" for legacy PrestoVectorSerde,
  //   "ArrowBatch" for Arrow IPC direct serving.
  std::shared_ptr<const facebook::velox::core::PlanNode> convert(
      const std::vector<uint8_t>& plan_bytes,
      const std::vector<split::SplitConfig>& splits,
      bool is_result_task = false,
      const std::string& result_serde_name = "Presto");

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

  // Return memory pool holders created during plan conversion.
  // Caller must keep these alive as long as the converted PlanNode tree
  // is in use (ValuesNode RowVector buffers reference these pools).
  std::vector<std::shared_ptr<facebook::velox::memory::MemoryPool>>&
  valuesPoolHolders() {
    return values_pool_holders_;
  }

 private:
  // Monotonically increasing node id counter for unique PlanNode ids.
  int next_node_id_ = 0;

  // Thread ownership tracking for @NotThreadSafe enforcement.
  // Set on first convert() call; subsequent calls assert same thread.
  // Debug-only: zero overhead in release builds.
  mutable std::thread::id owner_thread_id_;
  mutable bool owner_set_ = false;

  // Function reference → resolved function name map.
  // Built from Plan.extensions (SimpleExtensionDeclaration.ExtensionFunction)
  // and queried when converting scalar/aggregate function calls.
  std::unordered_map<uint32_t, std::string> function_map_;

  // TableScan planNodeId → output RowType mapping.
  // Populated during convertReadRel() for each TableScanNode created.
  // Consumed by DynamicFilterConverter to resolve column name → column index.
  std::unordered_map<std::string, facebook::velox::RowTypePtr>
      scan_node_output_types_;

  // Memory pools created during plan conversion (e.g., for ValuesNode vectors).
  // Must be kept alive as long as the converted PlanNode tree is in use,
  // because RowVector/FlatVector buffers reference the pool for deallocation.
  // PlanExecutionDriver must keep the converter alive alongside the runner.
  std::vector<std::shared_ptr<facebook::velox::memory::MemoryPool>>
      values_pool_holders_;

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
  // serde_name: serialization format name ("Presto" or "ArrowBatch").
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

  // Operator-specific converters.
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
  // followed by a single AggregationNode and a ProjectNode to strip groupId.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertMultiGroupingAggregate(
      const substrait::AggregateRel& aggregate_rel,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);

  // ExtensionSingleRel → UnnestNode (EXPLODE/POSEXPLODE) or VectorSearch.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertExtensionSingleRel(
      const substrait::ExtensionSingleRel& ext_rel,
      const std::vector<split::SplitConfig>& splits);

  // Vector search extension → Project(distance) + TopN.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  convertVectorSearchExtension(
      const std::string& detail_json,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);

  // ExtensionMultiRel → TableFunctionNode (table-valued functions).
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

  // Information about a distinct column in a multi-distinct aggregation.
  struct DistinctColumnInfo {
    // Indices of measures in AggregateRel.measures() that use this distinct column.
    std::vector<int> measure_indices;
    // Column name in the input schema that the DISTINCT operates on.
    std::string column_name;
    // Column index in the input schema.
    int column_index;
  };

  // Result of multi-distinct detection.
  struct MultiDistinctDetection {
    // True when ≥2 DISTINCT measures operate on different columns.
    bool is_multi_distinct = false;
    // Distinct column info for each unique distinct column.
    std::vector<DistinctColumnInfo> distinct_columns;
  };

  // Detect multi-distinct pattern: ≥2 DISTINCT measures on different columns.
  // Returns detection result with distinct column info for MarkDistinctNode insertion.
  // Single-DISTINCT does NOT trigger multi-distinct path.
  MultiDistinctDetection detectMultiDistinctColumns(
      const substrait::AggregateRel& aggregate_rel,
      const facebook::velox::RowTypePtr& input_type);

  // Build MarkDistinctNode chain + masked AggregationNode for multi-distinct.
  // Inserts one MarkDistinctNode per distinct column, then builds a single
  // AggregationNode with marker-masked aggregates.
  std::shared_ptr<const facebook::velox::core::PlanNode>
  buildMarkDistinctAggregation(
      const substrait::AggregateRel& aggregate_rel,
      const MultiDistinctDetection& multi_distinct,
      std::shared_ptr<const facebook::velox::core::PlanNode> input,
      const facebook::velox::RowTypePtr& input_type);

  // Extract grouping keys from the first grouping set of an AggregateRel.
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
  convertGroupingKeys(
      const substrait::AggregateRel& aggregate_rel,
      const facebook::velox::RowTypePtr& input_type);

  // Convert AggregateRel measures to Velox Aggregate descriptors.
  void convertAggregateMeasures(
      const substrait::AggregateRel& aggregate_rel,
      const facebook::velox::RowTypePtr& input_type,
      const std::unordered_set<std::string>& grouping_key_names,
      std::vector<std::string>& aggregate_names,
      std::vector<facebook::velox::core::AggregationNode::Aggregate>& aggregates);

  // Map Substrait aggregation phase to Velox AggregationNode::Step.
  facebook::velox::core::AggregationNode::Step resolveAggregationStep(
      const substrait::AggregateRel& aggregate_rel);

  // Detect pre-grouped keys when input is sorted on grouping keys.
  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
  detectPreGroupedKeys(
      const substrait::AggregateRel& aggregate_rel,
      const facebook::velox::RowTypePtr& input_type,
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
      const std::vector<std::string>& aggregate_names,
      const std::vector<facebook::velox::core::AggregationNode::Aggregate>& aggregates,
      std::shared_ptr<const facebook::velox::core::PlanNode> input);

  // --- JoinRel helpers ---

  // Build deduplicated output RowType for a join (left + right columns).
  facebook::velox::RowTypePtr buildJoinOutputType(
      const facebook::velox::RowTypePtr& left_type,
      const facebook::velox::RowTypePtr& right_type,
      std::unordered_set<std::string>& left_name_set,
      std::unordered_map<std::string, std::string>& right_rename_map);

  // Extract equi-join key pairs and residual conditions from a join expression.
  void extractJoinKeys(
      const substrait::Expression& expr,
      const facebook::velox::RowTypePtr& left_type,
      const facebook::velox::RowTypePtr& right_type,
      const facebook::velox::RowTypePtr& output_type,
      int left_field_count,
      std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& left_keys,
      std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& right_keys,
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& residual_conditions);

  // Build a post-join projection when right-side columns need renaming.
  std::shared_ptr<const facebook::velox::core::PlanNode> buildJoinProjection(
      const facebook::velox::RowTypePtr& right_type,
      const std::unordered_set<std::string>& left_name_set,
      const std::unordered_map<std::string, std::string>& right_rename_map,
      const std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& right_keys,
      std::shared_ptr<const facebook::velox::core::PlanNode> right,
      std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>& renamed_right_keys);

  // --- SetRel helpers ---

  // Build a UnionNode (gather) for UNION ALL.
  std::shared_ptr<const facebook::velox::core::PlanNode> buildUnionAll(
      std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>> sources);

  // Build chained semi/anti joins for INTERSECT or EXCEPT.
  std::shared_ptr<const facebook::velox::core::PlanNode> buildIntersectOrExcept(
      std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>> sources,
      facebook::velox::core::JoinType join_type);

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

  // Helper used by convertNamedStruct: consume the flattened names array in
  // pre-order DFS to rebuild a Velox RowType that preserves nested struct
  // field names.
  facebook::velox::TypePtr convertTypeConsumingNames(
      const substrait::Type& type,
      const google::protobuf::RepeatedPtrField<std::string>& names,
      int& idx);

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

  // Convert a Substrait Decimal literal to a Velox ConstantTypedExpr.
  std::shared_ptr<const facebook::velox::core::ITypedExpr> convertDecimalLiteral(
      const substrait::Expression_Literal_Decimal& dec, bool nullable);

  // Convert a Substrait List literal to a Velox ConstantTypedExpr wrapping an ArrayVector.
  std::shared_ptr<const facebook::velox::core::ITypedExpr> convertListLiteral(
      const substrait::Expression_Literal_List& list, bool nullable);

  // Insert implicit CASTs to align argument types to the target type.
  // Velox requires homogeneous types for arithmetic/comparison functions.
  static void alignArgumentTypes(
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& args,
      const facebook::velox::TypePtr& target_type);

  // Coerce comparison function arguments to their common type (not output type).
  static void alignComparisonArguments(
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& args);
};

}  // namespace pangrova::velox
