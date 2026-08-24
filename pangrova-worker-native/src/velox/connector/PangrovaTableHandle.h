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

#include "velox/connectors/Connector.h"
#include "velox/core/Expressions.h"

namespace pangrova::velox {

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
      std::shared_ptr<const facebook::velox::core::ITypedExpr>
          remainingFilter = nullptr)
      : facebook::velox::connector::ConnectorTableHandle(
            std::move(connectorId)),
        tableName_(std::move(tableName)),
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

 private:
  std::string tableName_;
  std::shared_ptr<const facebook::velox::core::ITypedExpr> remainingFilter_;
};

}  // namespace pangrova::velox
