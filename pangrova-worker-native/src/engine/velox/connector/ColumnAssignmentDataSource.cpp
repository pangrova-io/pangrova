#include "engine/velox/connector/ColumnAssignmentDataSource.h"

#include <stdexcept>

#include "engine/velox/connector/PangrovaTableHandle.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"

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
  if (join_filters_.empty() || rows->size() == 0) {
    return std::make_shared<RowVector>(rows->pool(), output_type_, rows->nulls(),
                                       rows->size(), rows->children());
  }
  SelectivityVector selected(rows->size());
  // A null root represents null in every projected column, regardless of child storage.
  const auto* rootNulls = rows->rawNulls();
  bool acceptsNull = true;
  if (rootNulls) {
    bits::andBits(selected.asMutableRange().bits(), rootNulls, 0, rows->size());
  }
  for (size_t channel = 0; channel < join_filters_.size(); ++channel) {
    if (const auto& filter = join_filters_[channel]) {
      acceptsNull &= filter->testNull();
      common::ScanSpec spec(source_type_->nameOf(channel));
      spec.setFilter(filter);
      spec.applyFilter(*rows->childAt(channel)->loadedVector(), rows->size(),
                       selected.asMutableRange().bits());
    }
  }
  if (rootNulls && acceptsNull) {
    bits::forEachUnsetBit(rootNulls, 0, rows->size(), [&](auto row) { selected.setValid(row, true); });
  }
  selected.updateBounds();
  const auto count = selected.countSelected();
  if (count == rows->size()) {
    return std::make_shared<RowVector>(rows->pool(), output_type_, rows->nulls(),
                                       rows->size(), rows->children());
  }
  if (count == 0) {
    // An empty filtered batch is not end-of-split: later input can still match.
    return std::static_pointer_cast<RowVector>(BaseVector::create(output_type_, 0, rows->pool()));
  }
  auto indices = allocateIndices(count, rows->pool());
  auto* target = indices->asMutable<vector_size_t>();
  auto nulls = rootNulls && acceptsNull ? allocateNulls(count, rows->pool()) : nullptr;
  vector_size_t offset = 0;
  selected.applyToSelected([&](auto row) {
    target[offset] = row;
    if (nulls) bits::setNull(nulls->asMutable<uint64_t>(), offset, rows->isNullAt(row));
    ++offset;
  });
  std::vector<VectorPtr> columns;
  for (const auto& child : rows->children()) {
    columns.push_back(BaseVector::wrapInDictionary(nullptr, indices, count, child));
  }
  return std::make_shared<RowVector>(rows->pool(), output_type_, std::move(nulls), count, std::move(columns));
}

void ColumnAssignmentDataSource::addDynamicFilter(
    column_index_t channel, const std::shared_ptr<common::Filter>& filter) {
  if (channel >= output_type_->size()) {
    throw std::out_of_range("Dynamic filter assignment channel out of range");
  }
  VELOX_CHECK_NOT_NULL(filter);
  if (join_filters_.empty()) {
    join_filters_.resize(output_type_->size());
  }
  join_filters_[channel] = filter;
}

const common::SubfieldFilters* ColumnAssignmentDataSource::getFilters() const {
  // Static reader predicates are applied by the wrapped source. An empty, non-null
  // map lets Velox distribute Join predicates to current and future split readers.
  static const common::SubfieldFilters empty;
  return &empty;
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
