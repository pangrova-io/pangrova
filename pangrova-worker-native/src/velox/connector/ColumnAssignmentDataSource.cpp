#include "velox/connector/ColumnAssignmentDataSource.h"

#include <stdexcept>

#include "velox/connector/PangrovaTableHandle.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {
using namespace facebook::velox;

RowTypePtr assignedSourceType(
    const RowTypePtr& outputType,
    const connector::ColumnHandleMap& assignments) {
  std::vector<std::string> names;
  names.reserve(outputType->size());
  for (size_t i = 0; i < outputType->size(); ++i) {
    const auto assignment = assignments.find(outputType->nameOf(i));
    if (assignment == assignments.end()) {
      throw std::invalid_argument("Missing exact connector column assignment: " +
                                  outputType->nameOf(i));
    }
    const auto handle = std::dynamic_pointer_cast<const PangrovaColumnHandle>(
        assignment->second);
    if (!handle || handle->name().empty()) {
      throw std::invalid_argument("Invalid Pangrova source column assignment");
    }
    names.push_back(handle->name());
  }
  return ROW(std::move(names), outputType->children());
}

core::TypedExprPtr assignedSourceFilter(
    const core::TypedExprPtr& filter,
    const RowTypePtr& outputType,
    const RowTypePtr& sourceType) {
  if (!filter) {
    return nullptr;
  }
  std::unordered_map<std::string, core::TypedExprPtr> mapping;
  for (size_t i = 0; i < outputType->size(); ++i) {
    mapping.emplace(outputType->nameOf(i),
        std::make_shared<core::FieldAccessTypedExpr>(
            sourceType->childAt(i), sourceType->nameOf(i)));
  }
  return filter->rewriteInputNames(mapping);
}

ColumnAssignmentDataSource::ColumnAssignmentDataSource(
    std::unique_ptr<connector::DataSource> source,
    RowTypePtr outputType,
    RowTypePtr sourceType)
    : source_(std::move(source)),
      output_type_(std::move(outputType)),
      source_type_(std::move(sourceType)) {
  if (!source_ || output_type_->size() != source_type_->size()) {
    throw std::invalid_argument("Invalid connector column assignment source");
  }
}

void ColumnAssignmentDataSource::addSplit(
    std::shared_ptr<connector::ConnectorSplit> split) {
  source_->addSplit(std::move(split));
}

std::optional<RowVectorPtr> ColumnAssignmentDataSource::next(
    uint64_t size, ContinueFuture& future) {
  auto batch = source_->next(size, future);
  if (!batch || !*batch) {
    return batch;
  }
  const auto& rows = *batch;
  if (*rows->type() != *source_type_) {
    throw std::runtime_error("Source batch does not match assigned logical schema");
  }
  return std::make_shared<RowVector>(rows->pool(), output_type_, rows->nulls(),
                                     rows->size(), rows->children());
}

void ColumnAssignmentDataSource::addDynamicFilter(
    column_index_t channel, const std::shared_ptr<common::Filter>& filter) {
  if (channel >= output_type_->size()) {
    throw std::out_of_range("Dynamic filter assignment channel out of range");
  }
  // Source and execution schemas have identical ordinal layouts.
  source_->addDynamicFilter(channel, filter);
}

const common::SubfieldFilters* ColumnAssignmentDataSource::getFilters() const {
  return source_->getFilters();
}
uint64_t ColumnAssignmentDataSource::getCompletedBytes() {
  return source_->getCompletedBytes();
}
uint64_t ColumnAssignmentDataSource::getCompletedRows() {
  return source_->getCompletedRows();
}
std::unordered_map<std::string, RuntimeMetric>
ColumnAssignmentDataSource::getRuntimeStats() {
  return source_->getRuntimeStats();
}
bool ColumnAssignmentDataSource::allPrefetchIssued() const {
  return source_->allPrefetchIssued();
}
int64_t ColumnAssignmentDataSource::estimatedRowSize() {
  return source_->estimatedRowSize();
}
void ColumnAssignmentDataSource::cancel() {
  source_->cancel();
}

}  // namespace pangrova::velox
