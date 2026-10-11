#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "retrieval/RetrievalFusion.h"

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace pangrova::velox {

enum class RetrievalMergeKind : uint8_t {
  kRrf = 1,
  kLinear = 2,
};

struct RetrievalMergeConfig {
  RetrievalMergeKind kind;
  size_t score_channel;
  size_t source_id_channel;
  std::vector<size_t> stable_key_channels;
  std::vector<pangrova::retrieval::SourceConfig> sources;
  uint64_t output_limit;
  uint64_t max_candidate_rows;
  uint64_t max_candidate_bytes;
  uint32_t max_fan_in;
  uint64_t rrf_k;
  bool physical_identity = false;
};

/** Unary Native relation that globally fuses canonical candidate rows. */
class RetrievalMergeNode final : public facebook::velox::core::PlanNode {
 public:
  RetrievalMergeNode(
      const facebook::velox::core::PlanNodeId& id,
      RetrievalMergeConfig config,
      std::shared_ptr<const facebook::velox::core::PlanNode> input)
      : PlanNode(id),
        config_(std::move(config)),
        output_type_(std::dynamic_pointer_cast<const facebook::velox::RowType>(
            input->outputType())),
        sources_{std::move(input)} {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return output_type_;
  }

  const std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>>&
  sources() const override {
    return sources_;
  }

  const RetrievalMergeConfig& config() const {
    return config_;
  }

  std::string_view name() const override {
    return "RetrievalMerge";
  }

  // Fusion must observe every candidate in the stage exactly once.  A custom
  // blocking operator instance is not shared across Velox drivers, therefore
  // allowing a multi-driver pipeline would silently produce one Top-K per
  // driver instead of the stage-global Top-K.
  bool requiresSingleThread() const override {
    return true;
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "kind="
           << (config_.kind == RetrievalMergeKind::kRrf ? "rrf" : "linear")
           << ", outputLimit=" << config_.output_limit
           << ", fanIn=" << config_.sources.size();
  }

  const RetrievalMergeConfig config_;
  const facebook::velox::RowTypePtr output_type_;
  const std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>>
      sources_;
};

class RetrievalMergeOperator final : public facebook::velox::exec::Operator {
 public:
  RetrievalMergeOperator(
      int32_t operator_id,
      facebook::velox::exec::DriverCtx* driver_ctx,
      const std::shared_ptr<const RetrievalMergeNode>& node);

  bool needsInput() const override;
  void addInput(facebook::velox::RowVectorPtr input) override;
  facebook::velox::RowVectorPtr getOutput() override;
  void noMoreInput() override;
  bool isFinished() override;
  facebook::velox::exec::BlockingReason isBlocked(
      facebook::velox::ContinueFuture* future) override;

 private:
  struct RowReference {
    size_t batch;
    facebook::velox::vector_size_t row;
  };

  std::string encodeStableKey(
      const facebook::velox::RowVectorPtr& input,
      facebook::velox::vector_size_t row) const;
  void materializeFusion();
  facebook::velox::RowVectorPtr buildOutputBatch(size_t begin, size_t count);

  const RetrievalMergeConfig config_;
  std::vector<facebook::velox::RowVectorPtr> input_batches_;
  std::vector<RowReference> row_references_;
  std::vector<pangrova::retrieval::Candidate> candidates_;
  std::vector<pangrova::retrieval::FusedCandidate> fused_;
  uint64_t candidate_bytes_{0};
  size_t output_offset_{0};
  bool fusion_materialized_{false};
};

class RetrievalMergeTranslator final
    : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;
};

void registerRetrievalMergeOperator();

}  // namespace pangrova::velox
