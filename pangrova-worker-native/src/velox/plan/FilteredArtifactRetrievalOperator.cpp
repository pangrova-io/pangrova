#include "velox/plan/FilteredArtifactRetrievalOperator.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

#include "velox/connector/PangrovaVeloxConnector.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/exec/Task.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/LazyVector.h"

namespace pangrova::velox {

FilteredArtifactRetrievalOperator::FilteredArtifactRetrievalOperator(
    int32_t operator_id, facebook::velox::exec::DriverCtx* driver_ctx,
    const std::shared_ptr<const FilteredArtifactRetrievalNode>& node)
    : Operator(driver_ctx, node->outputType(), operator_id, node->id(),
               "FilteredArtifactRetrieval"),
      config_(node->config()), memory_(pool()) {}

void FilteredArtifactRetrievalOperator::checkpoint() const {
  if (operatorCtx()->task()->getCancellationToken().isCancellationRequested()) {
    throw std::runtime_error("Filtered ANN task cancelled");
  }
}

void FilteredArtifactRetrievalOperator::initialize() {
  Operator::initialize();
  checkpoint();
  auto* driver = operatorCtx()->driverCtx();
  const auto& scan = config_.hydration_scan;
  auto* connector_pool = driver->task->addConnectorPoolLocked(
      planNodeId(), driver->pipelineId, driver->driverId, operatorType(),
      scan->tableHandle()->connectorId());
  connector_context_ = operatorCtx()->createConnectorQueryCtx(
      scan->tableHandle()->connectorId(), planNodeId(), connector_pool);
  loadSources();
  checkpoint();
}

void FilteredArtifactRetrievalOperator::loadSources() {
  std::vector<std::string> object_ids;
  for (const auto& split : config_.splits) {
    if (split.table_name != config_.artifact.source_id() ||
        split.source_snapshot != config_.artifact.source_snapshot() ||
        split.data_object_id.empty() || split.source_snapshot.empty() ||
        (split.file_format != split::FileFormatType::PARQUET &&
         split.file_format != split::FileFormatType::ORC) || split.row_position_filter_enabled ||
        (split.paimon && split.paimon->needs_merge)) {
      throw std::runtime_error("Filtered ANN split differs from its physical snapshot binding");
    }
    if (std::find(object_ids.begin(), object_ids.end(), split.data_object_id) == object_ids.end()) {
      object_ids.push_back(split.data_object_id);
    }
  }
  if (object_ids.empty() || config_.search_list_size == 0 ||
      object_ids.size() > config_.max_work / config_.search_list_size) {
    throw std::runtime_error("Filtered ANN source work exceeds its task grant");
  }
  size_t bound_objects = 0;
  for (const auto& object_id : object_ids) {
    std::vector<protocol::v2::ArtifactObject> objects;
    for (const auto& object : config_.artifact.split_objects()) {
      if (object.source_object_id() == object_id) {
        objects.push_back(object);
      }
    }
    bound_objects += objects.size();
    auto source = std::make_unique<artifact::ArtifactVectorSearch>(objects,
        static_cast<int64_t>(config_.query.size()),
        config_.max_read_bytes - usage_.downloaded_bytes, memory_,
        [this] { checkpoint(); }, config_.read_budget);
    usage_.downloaded_bytes += source->downloadedBytes();
    sources_.push_back(std::move(source));
  }
  if (bound_objects != static_cast<size_t>(config_.artifact.split_objects_size())) {
    throw std::runtime_error("Filtered ANN artifact contains objects outside this task partition");
  }
}

bool FilteredArtifactRetrievalOperator::needsInput() const {
  return !noMoreInput_;
}

void FilteredArtifactRetrievalOperator::addInput(facebook::velox::RowVectorPtr input) {
  checkpoint();
  if (noMoreInput_ || !input || input->childrenSize() != 4) {
    throw std::runtime_error("Filtered ANN requires exactly four physical eligibility fields");
  }
  facebook::velox::LazyVector::ensureLoadedRows(
      input, facebook::velox::SelectivityVector(input->size()));
  facebook::velox::DecodedVector source(*input->childAt(0));
  facebook::velox::DecodedVector snapshot(*input->childAt(1));
  facebook::velox::DecodedVector object(*input->childAt(2));
  facebook::velox::DecodedVector position(*input->childAt(3));
  for (facebook::velox::vector_size_t row = 0; row < input->size(); ++row) {
    checkpoint();
    if (source.isNullAt(row) || snapshot.isNullAt(row) || object.isNullAt(row) ||
        position.isNullAt(row) || position.valueAt<int64_t>(row) < 0 ||
        source.valueAt<facebook::velox::StringView>(row).str() != config_.artifact.source_id() ||
        snapshot.valueAt<facebook::velox::StringView>(row).str() != config_.artifact.source_snapshot()) {
      throw std::runtime_error("Filtered ANN active row has invalid signed physical identity");
    }
    const auto object_id = object.valueAt<facebook::velox::StringView>(row).str();
    auto selected = std::find_if(sources_.begin(), sources_.end(), [&](const auto& candidate) {
      return candidate->sourceObjectId() == object_id;
    });
    if (selected == sources_.end()) {
      throw std::runtime_error("Filtered ANN active row belongs to an unbound source object");
    }
    (*selected)->addEligible(static_cast<uint64_t>(position.valueAt<int64_t>(row)));
  }
  // Release payload-free eligibility input immediately. No user rows are retained.
}

void FilteredArtifactRetrievalOperator::noMoreInput() {
  Operator::noMoreInput();
}

void FilteredArtifactRetrievalOperator::searchSources() {
  selected_splits_ = config_.splits;
  for (const auto& source : sources_) {
    checkpoint();
    const uint64_t next_ceiling = usage_.expanded + config_.search_list_size;
    if (next_ceiling > config_.max_work) {
      throw std::runtime_error("Filtered ANN graph grant exhausted");
    }
    auto positions = source->search(config_.query,
        {config_.output_limit, next_ceiling, config_.max_work}, usage_,
        [this] { checkpoint(); });
    std::sort(positions.begin(), positions.end());
    for (auto& split : selected_splits_) {
      if (split.data_object_id == source->sourceObjectId()) {
        split.row_position_filter_enabled = true;
        split.row_position_allowlist.assign(positions.begin(), positions.end());
      }
    }
  }
  addRuntimeStat("filteredAnnExpandedNodes", facebook::velox::RuntimeCounter(usage_.expanded));
  addRuntimeStat("filteredAnnRescoredRows", facebook::velox::RuntimeCounter(usage_.rescored));
  searched_ = true;
}

bool FilteredArtifactRetrievalOperator::openNextSplit() {
  while (split_offset_ < selected_splits_.size()) {
    checkpoint();
    const auto& split = selected_splits_[split_offset_++];
    if (split.row_position_allowlist.empty()) {
      continue;
    }
    if (!reader_) {
      const auto& scan = config_.hydration_scan;
      auto connector = facebook::velox::connector::ConnectorRegistry::tryGet(
          *operatorCtx()->task()->queryCtx(), scan->tableHandle()->connectorId());
      if (!connector) {
        throw std::runtime_error("Filtered ANN hydration connector is absent");
      }
      reader_ = connector->createDataSource(scan->outputType(), scan->tableHandle(),
          scan->assignments(), connector_context_.get());
    }
    reader_->addSplit(std::make_shared<PangrovaConnectorSplit>(
        config_.hydration_scan->tableHandle()->connectorId(), split));
    split_open_ = true;
    return true;
  }
  return false;
}

facebook::velox::RowVectorPtr FilteredArtifactRetrievalOperator::getOutput() {
  checkpoint();
  if (!noMoreInput_ || finished_) {
    return nullptr;
  }
  if (!searched_) {
    searchSources();
  }
  while (true) {
    if (!split_open_ && !openNextSplit()) {
      reader_.reset();
      finished_ = true;
      return nullptr;
    }
    auto output = reader_->next(1024, blocking_future_);
    {
      auto actual = stats().wlock();
      actual->rawInputPositions = reader_->getCompletedRows();
      actual->rawInputBytes = reader_->getCompletedBytes();
    }
    checkpoint();
    if (!output) {
      if (!blocking_future_.valid()) {
        throw std::runtime_error("Filtered ANN hydration blocked without its owned future");
      }
      return nullptr;
    }
    if (!*output) {
      split_open_ = false;
      continue;
    }
    return std::move(*output);  // A zero-row batch is not EOF.
  }
}

bool FilteredArtifactRetrievalOperator::isFinished() { return finished_; }

facebook::velox::exec::BlockingReason FilteredArtifactRetrievalOperator::isBlocked(
    facebook::velox::ContinueFuture* future) {
  if (!blocking_future_.valid()) {
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  }
  *future = std::move(blocking_future_);
  return facebook::velox::exec::BlockingReason::kWaitForConnector;
}

void FilteredArtifactRetrievalOperator::close() {
  // Match the SDK TableScan owner protocol: an outstanding connector future
  // retains its Driver after Task termination. Keep reader buffers, context and
  // source leases alive until that actual Driver destroys this operator.
  if (reader_) {
    reader_->cancel();
  }
  Operator::close();
}

std::unique_ptr<facebook::velox::exec::Operator> FilteredArtifactRetrievalTranslator::toOperator(
    facebook::velox::exec::DriverCtx* ctx, int32_t id,
    const facebook::velox::core::PlanNodePtr& node) {
  auto filtered = std::dynamic_pointer_cast<const FilteredArtifactRetrievalNode>(node);
  return filtered ? std::make_unique<FilteredArtifactRetrievalOperator>(id, ctx, filtered) : nullptr;
}

void registerFilteredArtifactRetrievalOperator() {
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<FilteredArtifactRetrievalTranslator>());
}

}  // namespace pangrova::velox
