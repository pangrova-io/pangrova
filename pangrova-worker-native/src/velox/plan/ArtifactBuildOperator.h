// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "artifact/ArtifactWorkReportState.h"
#include "resource/TaskObjectReadBudget.h"
#include "pangrova/protocol/v2/execution_binding.pb.h"
#include "text/bm25/Bm25Segment.h"
#include "vector/diskann/VamanaGraph.h"
#include "vector/hnsw/HnswIndex.h"
#include "vector/ivfpq/IvfPqIndex.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace pangrova::artifact {
class TabularExportWriter;
}

namespace pangrova::velox {

struct ArtifactBuildTaskContext {
  std::string job_id;
  std::string page_id;
  uint64_t fencing_token{0};
  std::string scratch_directory;
  uint64_t max_input_bytes{0};
  uint64_t max_spill_bytes{0};
  uint64_t max_object_store_read_bytes{0};
  uint64_t max_object_store_write_bytes{0};
  std::shared_ptr<artifact::ArtifactWorkReportState> report_state;
  std::shared_ptr<resource::TaskObjectReadBudget> read_budget;
};

enum class ArtifactBuildKind : uint8_t {
  DISKANN,
  HNSW,
  IVF_PQ,
  BM25,
  BM25_REDUCE,
  BM25_FINALIZE,
  GENERATED_VALUES,
  TABULAR_EXPORT,
};

struct ArtifactBuildConfig {
  ArtifactBuildKind kind{ArtifactBuildKind::DISKANN};
  protocol::v2::ArtifactWriteBinding output;
  std::vector<protocol::v2::ArtifactObject> input_objects;
  ArtifactBuildTaskContext task;
  size_t row_position_channel{0};
  size_t vector_channel{0};
  size_t text_channel{0};
  size_t generated_value_channel{0};
  uint32_t generated_value_dimension{0};
  uint64_t max_rows{0};
  vector::diskann::VamanaConfig diskann;
  vector::hnsw::HnswConfig hnsw;
  vector::ivfpq::IvfPqConfig ivf_pq;
};

/** Unary, terminal Native background sink for one fenced artifact work page. */
class ArtifactBuildNode final : public facebook::velox::core::PlanNode {
 public:
  ArtifactBuildNode(
      const facebook::velox::core::PlanNodeId& id, ArtifactBuildConfig config,
      std::shared_ptr<const facebook::velox::core::PlanNode> input)
      : PlanNode(id),
        config_(std::move(config)),
        output_type_(facebook::velox::ROW({}, {})),
        sources_{std::move(input)} {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return output_type_;
  }
  const std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>>&
  sources() const override {
    return sources_;
  }
  const ArtifactBuildConfig& config() const { return config_; }
  std::string_view name() const override { return "ArtifactBuild"; }
  bool requiresSingleThread() const override { return true; }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "artifact=" << config_.output.artifact_id()
           << ", generation=" << config_.output.generation()
           << ", format=" << config_.output.format().format_id();
  }

  const ArtifactBuildConfig config_;
  const facebook::velox::RowTypePtr output_type_;
  const std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>>
      sources_;
};

class ArtifactBuildOperator final : public facebook::velox::exec::Operator {
 public:
  ArtifactBuildOperator(int32_t operator_id,
                        facebook::velox::exec::DriverCtx* driver_ctx,
                        const std::shared_ptr<const ArtifactBuildNode>& node);
  ~ArtifactBuildOperator() override;

  bool needsInput() const override;
  void addInput(facebook::velox::RowVectorPtr input) override;
  facebook::velox::RowVectorPtr getOutput() override;
  void noMoreInput() override;
  bool isFinished() override;
  facebook::velox::exec::BlockingReason isBlocked(
      facebook::velox::ContinueFuture* future) override;

 private:
  void openExportWriter();
  void appendVectors(const facebook::velox::RowVectorPtr& input);
  void buildAndPublish();
  void mergeBm25Statistics();

  const ArtifactBuildConfig config_;
  const facebook::velox::RowTypePtr export_input_type_;
  std::vector<uint64_t> row_positions_;
  std::vector<float> vectors_;
  std::vector<uint8_t> generated_validity_;
  text::bm25::Bm25SegmentBuilder bm25_;
  std::unordered_set<uint64_t> unique_row_positions_;
  int64_t dimension_{0};
  uint64_t input_bytes_{0};
  uint64_t export_rows_{0};
  uint64_t export_peak_batch_bytes_{0};
  std::chrono::steady_clock::time_point export_started_{};
  std::filesystem::path export_scratch_path_;
  std::unique_ptr<pangrova::artifact::TabularExportWriter> export_writer_;
  bool published_{false};
};

class ArtifactBuildTranslator final
    : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx, int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;
};

void registerArtifactBuildOperator();

}  // namespace pangrova::velox
