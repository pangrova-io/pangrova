// PangrovaTableHandle: Velox ConnectorTableHandle carrying Pangrova-specific
// metadata including an optional remaining filter expression for predicate
// pushdown to the DWIO reader layer.
//
// PangrovaColumnHandle: Velox ColumnHandle wrapping a column name.
//
// Shared between VeloxPlanConverter (producer) and PangrovaVeloxConnector
// (consumer). The remaining filter, when present, is passed through to
// RowReaderOptions::setFilter() enabling row group statistics-based pruning.
//
// Placement: velox/ — Velox integration layer.
#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "velox/connectors/Connector.h"
#include "velox/core/Expressions.h"

namespace pangrova::velox {

// Reserved Native-only column synthesized by the file reader. Plans may use it
// for physical row locators, but connectors must never resolve it as a user
// table column.
inline constexpr std::string_view kPhysicalRowPositionColumnName =
    "$pangrova$row_position";
inline constexpr std::string_view kSourceObjectIdColumnName =
    "$pangrova$source_object_id";
inline constexpr std::string_view kMaterializedRetrievalScoreColumnName =
    "$pangrova$retrieval_score";

// Column handle wrapping a single column name for Velox connector API.
class PangrovaColumnHandle : public facebook::velox::connector::ColumnHandle {
 public:
  explicit PangrovaColumnHandle(std::string columnName)
      : columnName_(std::move(columnName)) {}

  const std::string& name() const override {
    return columnName_;
  }

 private:
  std::string columnName_;
};

// Table handle carrying table name and optional remaining filter.
// The remaining filter originates from the Substrait ReadRel's filter
// expression, converted to a Velox ITypedExpr by VeloxPlanConverter.
// PangrovaVeloxConnector::createDataSource() extracts this filter and
// forwards it to PangrovaVeloxDataSource for DWIO reader-level pushdown.
class PangrovaTableHandle
    : public facebook::velox::connector::ConnectorTableHandle {
 public:
  PangrovaTableHandle(
      std::string connectorId,
      std::string tableName,
      std::string planNodeId,
      facebook::velox::RowTypePtr scanSchema,
      std::shared_ptr<const facebook::velox::core::ITypedExpr>
          remainingFilter = nullptr)
      : facebook::velox::connector::ConnectorTableHandle(
            std::move(connectorId)),
        tableName_(std::move(tableName)),
        planNodeId_(std::move(planNodeId)),
        scanSchema_(std::move(scanSchema)),
        remainingFilter_(std::move(remainingFilter)) {}

  const std::string& name() const override {
    return tableName_;
  }

  // Returns the remaining filter expression for predicate pushdown.
  // nullptr when no filter is available for this scan.
  const std::shared_ptr<const facebook::velox::core::ITypedExpr>&
  remainingFilter() const {
    return remainingFilter_;
  }

  const std::string& planNodeId() const {
    return planNodeId_;
  }
  const facebook::velox::RowTypePtr& scanSchema() const { return scanSchema_; }

 private:
  std::string tableName_;
  std::string planNodeId_;
  facebook::velox::RowTypePtr scanSchema_;
  std::shared_ptr<const facebook::velox::core::ITypedExpr> remainingFilter_;
};

}  // namespace pangrova::velox
