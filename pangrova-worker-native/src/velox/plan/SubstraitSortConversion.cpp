// Canonical sort and fetch over the actual input layout.
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "velox/plan/CandidateContractValidation.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <functional>
#include <limits>
#include <unordered_set>

#include <openssl/evp.h>

#include "compat/NativeFeatureDetection.h"
#include "capability/NativeCapabilityCatalog.h"
#include "artifact/GeneratedValueSegment.h"
#include "artifact/ImmutableArtifactCache.h"
#include "artifact/RowLocatorSidecar.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/connector/PangrovaVeloxConnector.h"
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
#include "velox/plan/TableFunctionNode.h"
#include "velox/plan/RetrievalMergeOperator.h"
#include "velox/plan/ArtifactBuildOperator.h"
#include "velox/plan/GeneratedValuesOverlayOperator.h"
#include "velox/runtime/NativeEngineLifecycle.h"
#include "velox/runtime/TableFunctionRegistry.h"

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

}

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

  int64_t offset = 0;
  switch (fetch_rel.offset_mode_case()) {
    case substrait::FetchRel::kOffsetExpr:
      offset = extractFetchInteger(fetch_rel.offset_expr(), "offset")
                   .value_or(0);
      break;
    case substrait::FetchRel::kOffset:
      throw std::runtime_error(
          "VeloxPlanConverter: deprecated FetchRel.offset is not accepted; "
          "use offset_expr");
    case substrait::FetchRel::OFFSET_MODE_NOT_SET:
      break;
  }

  std::optional<int64_t> bounded_count;
  switch (fetch_rel.count_mode_case()) {
    case substrait::FetchRel::kCountExpr:
      bounded_count = extractFetchInteger(fetch_rel.count_expr(), "count");
      break;
    case substrait::FetchRel::kCount:
      throw std::runtime_error(
          "VeloxPlanConverter: deprecated FetchRel.count is not accepted; "
          "use count_expr");
    case substrait::FetchRel::COUNT_MODE_NOT_SET:
      break;
  }

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
  // In Velox parallel mode, each driver independently executes the plan.
  // A single TopNNode(isPartial=false) produces per-driver top-N results
  // that are concatenated without global merge — yielding N×max_drivers
  // rows in arbitrary order.  Splitting into partial + final ensures each
  // driver emits its local top-N (partial), then a single-threaded final
  // TopNNode merges all partial results into the correct global top-N.
  // This is the same pattern as kPartial→kFinal for AggregationNode.
  auto order_by = std::dynamic_pointer_cast<
      const facebook::velox::core::OrderByNode>(input);
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
    return std::make_shared<facebook::velox::core::TopNNode>(
        nextNodeId(),
        order_by->sortingKeys(),
        order_by->sortingOrders(),
        static_cast<int32_t>(count),
        /*isPartial=*/false,
        partialTopN);
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
  return std::make_shared<facebook::velox::core::LimitNode>(
      nextNodeId(),
      offset,
      count,
      /*isPartial=*/false,
      partialLimit);
}

// ---------------------------------------------------------------------------
// WindowRel → WindowNode
// ---------------------------------------------------------------------------


}  // namespace pangrova::velox

