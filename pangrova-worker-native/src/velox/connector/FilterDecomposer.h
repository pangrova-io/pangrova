// FilterDecomposer: Decomposes Velox filter expressions into per-column
// SubfieldFilters on ScanSpec for DWIO reader-level predicate pushdown.
//
// Supports: AND, IS NULL, IS NOT NULL, OR-of-eq (IN list), BETWEEN,
// and comparison operators (eq, neq, lt, lte, gt, gte) on integer,
// floating-point, varchar, varbinary, short decimal, and Parquet UUID columns.
//
// Unsupported predicates are silently skipped — the FilterNode above
// handles them at row level. This is a best-effort optimization.
//
// Stateless free functions — safe to call from any thread.
//
// Placement: velox/connector/ — Velox integration utilities.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "velox/core/Expressions.h"
#include "velox/type/Type.h"

// Forward declare ScanSpec to avoid pulling heavy DWIO headers.
namespace facebook::velox::common {
class ScanSpec;
}  // namespace facebook::velox::common

namespace pangrova::velox {

// Decompose a filter expression tree into SubfieldFilters on the ScanSpec.
// Returns the number of filters successfully pushed down. UUID byte filters
// require a Parquet UUID read plan that validated the annotation and width.
int decomposeFilterToScanSpec(
    const std::shared_ptr<const facebook::velox::core::ITypedExpr>& filter,
    const std::shared_ptr<facebook::velox::common::ScanSpec>& scanSpec,
    const facebook::velox::RowTypePtr& outputType,
    bool parquetUuidBytes = false);

// Enable dictionary-aware filter evaluation for specified columns.
// Velox SelectiveColumnReader automatically evaluates pushed-down filters
// on dictionary indices for dictionary-encoded columns, yielding 3-5x
// speedup on low-cardinality columns (status, region, category).
void enableDictionaryFilteringForColumns(
    const std::shared_ptr<facebook::velox::common::ScanSpec>& scanSpec,
    const std::vector<std::string>& dictionaryEncodedColumns);

}  // namespace pangrova::velox
