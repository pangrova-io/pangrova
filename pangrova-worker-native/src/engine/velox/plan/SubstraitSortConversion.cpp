// Canonical sort and fetch over the actual input layout.
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "engine/velox/plan/CanonicalOrdinalIdentity.h"
#include "engine/velox/plan/CandidateContractValidation.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include <openssl/evp.h>

#include "engine/compat/NativeFeatureDetection.h"
#include "capabilities/capability/NativeCapabilityCatalog.h"
#include "capabilities/artifact/GeneratedValueSegment.h"
#include "capabilities/artifact/ImmutableArtifactCache.h"
#include "capabilities/artifact/RowLocatorSidecar.h"
#include "engine/executor/NativeUnsupportedFeatureException.h"
#include "engine/velox/connector/PangrovaTableHandle.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/type/Type.h"

#include "substrait/algebra.pb.h"

#include <google/protobuf/wrappers.pb.h>
#include <sstream>

#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "engine/velox/plan/TableFunctionNode.h"
#include "engine/velox/plan/RetrievalMergeOperator.h"
#include "engine/velox/plan/ArtifactBuildOperator.h"
#include "engine/velox/plan/GeneratedValuesOverlayOperator.h"
#include "engine/velox/runtime/NativeEngineLifecycle.h"
#include "engine/velox/runtime/TableFunctionRegistry.h"

namespace pangrova::velox {
namespace {
std::optional<int64_t> extractFetchInteger(
    const substrait::Expression& expression,
    const char* field_name) {
  if (!expression.has_literal()) {
    throw std::runtime_error(
        std::string("VeloxPlanConverter: FetchRel ") + field_name +
        " must be a constant integer expression");
  }
  const auto& literal = expression.literal();
  if (literal.has_null()) {
    return std::nullopt;
  }
  int64_t value;
  if (literal.has_i8()) {
    value = literal.i8();
  } else if (literal.has_i16()) {
    value = literal.i16();
  } else if (literal.has_i32()) {
    value = literal.i32();
  } else if (literal.has_i64()) {
    value = literal.i64();
  } else {
    throw std::runtime_error(
        std::string("VeloxPlanConverter: FetchRel ") + field_name +
        " must have an integer literal type");
  }
  if (value < 0) {
    throw std::runtime_error(
        std::string("VeloxPlanConverter: FetchRel ") + field_name +
        " must be non-negative");
  }
  return value;
}

struct FetchBounds {
  int64_t offset{0};
  std::optional<int64_t> count;
};

FetchBounds fetchBounds(const substrait::FetchRel& fetch) {
  FetchBounds result;
  switch (fetch.offset_mode_case()) {
    case substrait::FetchRel::kOffsetExpr:
      result.offset = extractFetchInteger(fetch.offset_expr(), "offset").value_or(0);
      break;
    case substrait::FetchRel::kOffset:
      throw std::runtime_error("VeloxPlanConverter: deprecated FetchRel.offset is not accepted; use offset_expr");
    case substrait::FetchRel::OFFSET_MODE_NOT_SET:
      break;
  }
  switch (fetch.count_mode_case()) {
    case substrait::FetchRel::kCountExpr:
      result.count = extractFetchInteger(fetch.count_expr(), "count");
      break;
    case substrait::FetchRel::kCount:
      throw std::runtime_error("VeloxPlanConverter: deprecated FetchRel.count is not accepted; use count_expr");
    case substrait::FetchRel::COUNT_MODE_NOT_SET:
      break;
  }
  return result;
}

struct RestoredSort {
  std::shared_ptr<const facebook::velox::core::OrderByNode> node;
  std::vector<std::shared_ptr<const facebook::velox::core::ProjectNode>> projections;
};

// Only exact prefix restoration: no arbitrary computation, column reordering,
// duplication, join, exchange or function traversal is an ordering proof.
RestoredSort restoredSort(facebook::velox::core::PlanNodePtr input) {
  RestoredSort result;
  while (const auto project = std::dynamic_pointer_cast<const facebook::velox::core::ProjectNode>(input)) {
    if (project->sources().size() != 1) return {};
    const auto source = project->sources().front();
    if (project->projections().size() > source->outputType()->size()) return {};
    for (size_t ordinal = 0; ordinal < project->projections().size(); ++ordinal) {
      const auto field = std::dynamic_pointer_cast<const facebook::velox::core::FieldAccessTypedExpr>(
          project->projections().at(ordinal));
      if (!field || !field->inputs().empty() || field->name() != source->outputType()->nameOf(ordinal) ||
          !field->type()->equivalent(*source->outputType()->childAt(ordinal))) return {};
    }
    result.projections.push_back(project);
    input = source;
  }
  result.node = std::dynamic_pointer_cast<const facebook::velox::core::OrderByNode>(input);
  return result;
}

}

facebook::velox::core::SortOrder VeloxPlanConverter::convertSortOrder(
    const substrait::SortField& sort_field) {
  if (!sort_field.has_direction()) {
    throw std::runtime_error("Sort requires an explicit ordinary direction; custom comparators are unsupported");
  }
  switch (sort_field.direction()) {
      case substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST:
        return {true, true};
      case substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST:
        return {true, false};
      case substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST:
        return {false, true};
      case substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST:
        return {false, false};
      default:
        throw std::runtime_error("Sort direction is missing, unknown or not an ordinary four-direction order");
  }
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
  auto names = input_type->names();
  std::vector<facebook::velox::core::TypedExprPtr> projections;
  for (size_t ordinal = 0; ordinal < input_type->size(); ++ordinal) {
    projections.push_back(std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
        input_type->childAt(ordinal), input_type->nameOf(ordinal)));
  }
  if (sort_rel.sorts_size() == 0) throw std::runtime_error("SortRel requires at least one ordering key");

  for (const auto& sort_field : sort_rel.sorts()) {
    if (!sort_field.has_expr()) throw std::runtime_error("SortField requires an expression");
    sorting_orders.push_back(convertSortOrder(sort_field));
    auto value = aggregateInputValue(sort_field.expr(), input_type, names, projections, false);
    const auto field = std::dynamic_pointer_cast<const facebook::velox::core::FieldAccessTypedExpr>(value);
    if (!field || !field->inputs().empty()) throw std::runtime_error("Prepared sorting key must be a direct typed field");
    sorting_keys.push_back(field);
  }
  const bool computed = names.size() != input_type->size();
  if (computed) input = std::make_shared<facebook::velox::core::ProjectNode>(
      nextNodeId(), std::move(names), std::move(projections), input);

  // Note: OrderByNode is NOT split into partial→final here because
  // SortRel is almost always followed by FetchRel (ORDER BY + LIMIT).
  // convertFetchRel detects OrderByNode as input and optimizes to
  // TopNNode(partial=true) → TopNNode(partial=false), which handles
  // parallel-safe sorting with limit.  Splitting OrderBy here would
  // conflict with that optimization.
  facebook::velox::core::PlanNodePtr ordered = std::make_shared<facebook::velox::core::OrderByNode>(
      nextNodeId(),
      sorting_keys,
      sorting_orders,
      /*isPartial=*/false,
      input);
  if (!computed) return ordered;
  std::vector<facebook::velox::core::TypedExprPtr> restored;
  for (size_t ordinal = 0; ordinal < input_type->size(); ++ordinal) {
    restored.push_back(std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
        input_type->childAt(ordinal), input_type->nameOf(ordinal)));
  }
  return std::make_shared<facebook::velox::core::ProjectNode>(
      nextNodeId(), input_type->names(), std::move(restored), ordered);
}

// ---------------------------------------------------------------------------
// FetchRel → LimitNode / TopNNode
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertFetchRel(
    const substrait::FetchRel& fetch_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(fetch_rel.input(), splits);

  const auto bounds = fetchBounds(fetch_rel);
  const auto offset = bounds.offset;
  const auto& bounded_count = bounds.count;

  if (!bounded_count.has_value() && offset == 0) {
    return input;
  }
  if (bounded_count.has_value() && *bounded_count == 0) {
    auto false_expr =
        std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::BOOLEAN(), facebook::velox::variant(false));
    return std::make_shared<facebook::velox::core::FilterNode>(
        nextNodeId(), std::move(false_expr), input);
  }
  const int64_t count = bounded_count.value_or(
      std::numeric_limits<int64_t>::max());

  // If the input is an OrderByNode, optimize to TopNNode.
  // Parallel-safe TopN: split into isPartial=true → isPartial=false.
  // A final TopN requires one driver for its entire pipeline. The real
  // gather is the pipeline boundary: producers may run partial TopN in
  // parallel, then exactly one consumer merges those local candidates.
  // This is the same pattern as kPartial→kFinal for AggregationNode.
  const auto sorting = restoredSort(input);
  const auto& order_by = sorting.node;
  constexpr int64_t kMaxTopN =
      static_cast<int64_t>(std::numeric_limits<int32_t>::max());
  if (order_by && offset == 0 && count <= kMaxTopN) {
    auto partialTopN = std::make_shared<facebook::velox::core::TopNNode>(
        nextNodeId(),
        order_by->sortingKeys(),
        order_by->sortingOrders(),
        static_cast<int32_t>(count),
        /*isPartial=*/true,
        order_by->sources()[0]);
    const auto gathered = facebook::velox::core::LocalPartitionNode::gather(nextNodeId(), {partialTopN});
    facebook::velox::core::PlanNodePtr result = std::make_shared<facebook::velox::core::TopNNode>(
        nextNodeId(),
        order_by->sortingKeys(),
        order_by->sortingOrders(),
        static_cast<int32_t>(count),
        /*isPartial=*/false,
        gathered);
    for (auto it = sorting.projections.rbegin(); it != sorting.projections.rend(); ++it) {
      result = std::make_shared<facebook::velox::core::ProjectNode>(nextNodeId(),
          (*it)->outputType()->names(), (*it)->projections(), result);
    }
    if (result->outputType()->names() != input->outputType()->names() ||
        !result->outputType()->equivalent(*input->outputType())) {
      throw std::runtime_error("Fetch Top-N rewrite changed the declared sort output schema");
    }
    return result;
  }

  // Parallel-safe Limit: each driver may emit at most offset + count rows,
  // but the offset itself is applied exactly once by the final node. Applying
  // offset in every partial driver silently drops valid rows.
  const int64_t partial_count =
      count > std::numeric_limits<int64_t>::max() - offset
      ? std::numeric_limits<int64_t>::max()
      : count + offset;
  auto partialLimit = std::make_shared<facebook::velox::core::LimitNode>(
      nextNodeId(),
      0,
      partial_count,
      /*isPartial=*/true,
      input);
  const auto gathered = facebook::velox::core::LocalPartitionNode::gather(nextNodeId(), {partialLimit});
  return std::make_shared<facebook::velox::core::LimitNode>(
      nextNodeId(),
      offset,
      count,
      /*isPartial=*/false,
      gathered);
}

// ---------------------------------------------------------------------------
// WindowRel → WindowNode
// ---------------------------------------------------------------------------


}  // namespace pangrova::velox
