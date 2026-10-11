#pragma once

#include <array>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "artifact/ArtifactVectorSearch.h"
#include "split/SplitConfig.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/plan/OperatorMemoryResource.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace pangrova::velox {

struct FilteredArtifactRetrievalConfig {
  protocol::v2::ArtifactBinding artifact;
  std::vector<float> query;
  uint64_t output_limit = 0;
  uint64_t search_list_size = 0;
  uint64_t max_work = 0;
  uint64_t max_read_bytes = 0;
  std::shared_ptr<resource::TaskObjectReadBudget> read_budget;
  std::vector<split::SplitConfig> splits;
  std::shared_ptr<const facebook::velox::core::TableScanNode> hydration_scan;
};

/** One live Task collects SQL eligibility, searches and hydrates physical rows. */
class FilteredArtifactRetrievalNode final : public facebook::velox::core::PlanNode {
 public:
  FilteredArtifactRetrievalNode(
      const facebook::velox::core::PlanNodeId& id,
      FilteredArtifactRetrievalConfig config,
      facebook::velox::core::PlanNodePtr input)
      : PlanNode(id), config_(std::move(config)),
        output_type_(config_.hydration_scan ? config_.hydration_scan->outputType()
            : throw std::invalid_argument("Filtered ANN requires its actual hydration scan")),
        sources_{std::move(input)} {
    if (!sources_.front() || !config_.read_budget || config_.output_limit == 0 ||
        config_.search_list_size == 0 || config_.max_work < config_.output_limit) {
      throw std::invalid_argument("Filtered ANN requires its actual input and admitted work owners");
    }
  }
  const facebook::velox::RowTypePtr& outputType() const override { return output_type_; }
  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override { return sources_; }
  const FilteredArtifactRetrievalConfig& config() const { return config_; }
  std::string_view name() const override { return "FilteredArtifactRetrieval"; }
  bool requiresSingleThread() const override { return true; }
 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "outputLimit=" << config_.output_limit
           << ", perSourceExploration=" << config_.search_list_size;
  }
  const FilteredArtifactRetrievalConfig config_;
  const facebook::velox::RowTypePtr output_type_;
  const std::vector<facebook::velox::core::PlanNodePtr> sources_;
};

class FilteredArtifactRetrievalOperator final : public facebook::velox::exec::Operator {
 public:
  FilteredArtifactRetrievalOperator(int32_t operator_id,
      facebook::velox::exec::DriverCtx* driver_ctx,
      const std::shared_ptr<const FilteredArtifactRetrievalNode>& node);
  void initialize() override;
  bool needsInput() const override;
  void addInput(facebook::velox::RowVectorPtr input) override;
  void noMoreInput() override;
  facebook::velox::RowVectorPtr getOutput() override;
  bool isFinished() override;
  facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override;
  void close() override;
 private:
  void checkpoint() const;
  void loadSources();
  void searchSources();
  bool openNextSplit();
  const FilteredArtifactRetrievalConfig config_;
  OperatorMemoryResource memory_;
  std::vector<std::unique_ptr<artifact::ArtifactVectorSearch>> sources_;
  artifact::VectorSearchUsage usage_;
  std::vector<split::SplitConfig> selected_splits_;
  std::shared_ptr<facebook::velox::connector::ConnectorQueryCtx> connector_context_;
  std::unique_ptr<facebook::velox::connector::DataSource> reader_;
  facebook::velox::ContinueFuture blocking_future_{facebook::velox::ContinueFuture::makeEmpty()};
  size_t split_offset_ = 0;
  bool searched_ = false;
  bool split_open_ = false;
  bool finished_ = false;
};

class FilteredArtifactRetrievalTranslator final
    : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx, int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;
};

void registerFilteredArtifactRetrievalOperator();

}  // namespace pangrova::velox
