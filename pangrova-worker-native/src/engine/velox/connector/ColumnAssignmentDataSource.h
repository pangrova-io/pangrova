// Exact connector column assignments bridge execution aliases to source names.
// Velox join predicates address output ordinals before aliases are restored.
#pragma once

#include <vector>

#include "velox/connectors/Connector.h"
#include "velox/core/Expressions.h"

namespace pangrova::velox {

facebook::velox::RowTypePtr assignedSourceType(
    const facebook::velox::RowTypePtr& outputType,
    const facebook::velox::connector::ColumnHandleMap& assignments);

facebook::velox::core::TypedExprPtr assignedSourceFilter(
    const facebook::velox::core::TypedExprPtr& filter,
    const facebook::velox::RowTypePtr& outputType,
    const facebook::velox::RowTypePtr& sourceType);

class ColumnAssignmentDataSource final
    : public facebook::velox::connector::DataSource {
 public:
  ColumnAssignmentDataSource(
      std::unique_ptr<facebook::velox::connector::DataSource> source,
      facebook::velox::RowTypePtr outputType,
      facebook::velox::RowTypePtr sourceType);

  void addSplit(std::shared_ptr<facebook::velox::connector::ConnectorSplit>) override;
  std::optional<facebook::velox::RowVectorPtr> next(
      uint64_t size, facebook::velox::ContinueFuture& future) override;
  void addDynamicFilter(
      facebook::velox::column_index_t,
      const std::shared_ptr<facebook::velox::common::Filter>&) override;
  const facebook::velox::common::SubfieldFilters* getFilters() const override;
  uint64_t getCompletedBytes() override;
  uint64_t getCompletedRows() override;
  std::unordered_map<std::string, facebook::velox::RuntimeMetric>
  getRuntimeStats() override;
  bool allPrefetchIssued() const override;
  int64_t estimatedRowSize() override;
  void cancel() override;

 private:
  std::unique_ptr<facebook::velox::connector::DataSource> source_;
  facebook::velox::RowTypePtr output_type_;
  facebook::velox::RowTypePtr source_type_;
  // Velox delivers the complete, merged filter for an output ordinal under the scan driver lock.
  std::vector<std::shared_ptr<facebook::velox::common::Filter>> join_filters_;
};

}  // namespace pangrova::velox
