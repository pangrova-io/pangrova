// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "capabilities/artifact/ImmutableArtifactCache.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace pangrova::velox {

struct GeneratedValuesForSource final {
  uint32_t dimension{0};
  std::vector<float> values;
  std::vector<uint8_t> validity;
  std::unordered_map<uint64_t, uint64_t> row_ordinals;
  // Cache leases keep both checksum-verified objects pinned for the task.
  std::vector<artifact::MaterializedArtifact> leases;
};

struct GeneratedValuesOverlayConfig final {
  size_t source_object_channel{0};
  size_t row_position_channel{0};
  uint64_t max_fill_keys{0};
  std::unordered_map<std::string, GeneratedValuesForSource> sources;
};

struct GeneratedValueClaim final {
  std::string source_object_id;
  uint64_t ordinal{0};
};

/** Task-local duplicate and resource guard shared by all parallel drivers. */
class GeneratedValuesOverlayRuntimeState final {
 public:
  explicit GeneratedValuesOverlayRuntimeState(
      const GeneratedValuesOverlayConfig& config);

  void claim(const std::vector<GeneratedValueClaim>& claims);

 private:
  std::mutex mutex_;
  uint64_t max_fill_keys_{0};
  uint64_t claimed_keys_{0};
  std::unordered_map<std::string, std::vector<uint8_t>> claimed_ordinals_;
};

/** Streaming, exact source-object/physical-position generated-value join. */
class GeneratedValuesOverlayNode final
    : public facebook::velox::core::PlanNode {
 public:
  GeneratedValuesOverlayNode(
      const facebook::velox::core::PlanNodeId& id,
      GeneratedValuesOverlayConfig config,
      facebook::velox::RowTypePtr output_type,
      std::shared_ptr<const facebook::velox::core::PlanNode> input)
      : GeneratedValuesOverlayNode(
            id,
            std::make_shared<const GeneratedValuesOverlayConfig>(
                std::move(config)),
            std::move(output_type), std::move(input)) {}

  GeneratedValuesOverlayNode(
      const facebook::velox::core::PlanNodeId& id,
      std::shared_ptr<const GeneratedValuesOverlayConfig> config,
      facebook::velox::RowTypePtr output_type,
      std::shared_ptr<const facebook::velox::core::PlanNode> input)
      : PlanNode(id),
        config_(requireConfig(std::move(config))),
        runtime_(
            std::make_shared<GeneratedValuesOverlayRuntimeState>(*config_)),
        output_type_(std::move(output_type)),
        sources_{std::move(input)} {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return output_type_;
  }
  const std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>>&
  sources() const override {
    return sources_;
  }
  const std::shared_ptr<const GeneratedValuesOverlayConfig>& config() const {
    return config_;
  }
  const std::shared_ptr<GeneratedValuesOverlayRuntimeState>& runtime() const {
    return runtime_;
  }
  std::string_view name() const override { return "GeneratedValuesOverlay"; }

 private:
  static std::shared_ptr<const GeneratedValuesOverlayConfig> requireConfig(
      std::shared_ptr<const GeneratedValuesOverlayConfig> config) {
    if (!config) {
      throw std::invalid_argument(
          "GeneratedValuesOverlayNode requires an immutable config");
    }
    return config;
  }

  void addDetails(std::stringstream& stream) const override {
    stream << "source_objects=" << config_->sources.size()
           << ", max_fill_keys=" << config_->max_fill_keys;
  }

  // All drivers share immutable materialized artifacts and locator maps. Each
  // driver only owns its current output batch, so memory remains O(artifact)
  // rather than O(artifact * drivers).
  const std::shared_ptr<const GeneratedValuesOverlayConfig> config_;
  const std::shared_ptr<GeneratedValuesOverlayRuntimeState> runtime_;
  const facebook::velox::RowTypePtr output_type_;
  const std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>>
      sources_;
};

class GeneratedValuesOverlayOperator final
    : public facebook::velox::exec::Operator {
 public:
  GeneratedValuesOverlayOperator(
      int32_t operator_id, facebook::velox::exec::DriverCtx* driver_ctx,
      const std::shared_ptr<const GeneratedValuesOverlayNode>& node);

  bool needsInput() const override;
  void addInput(facebook::velox::RowVectorPtr input) override;
  facebook::velox::RowVectorPtr getOutput() override;
  void noMoreInput() override;
  bool isFinished() override;
  facebook::velox::exec::BlockingReason isBlocked(
      facebook::velox::ContinueFuture* future) override;

 private:
  const std::shared_ptr<const GeneratedValuesOverlayConfig> config_;
  const std::shared_ptr<GeneratedValuesOverlayRuntimeState> runtime_;
  facebook::velox::RowVectorPtr output_;
};

class GeneratedValuesOverlayTranslator final
    : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx, int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;
};

void registerGeneratedValuesOverlayOperator();

}  // namespace pangrova::velox
