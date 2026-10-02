// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "velox/plan/GeneratedValuesOverlayOperator.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox {

GeneratedValuesOverlayRuntimeState::GeneratedValuesOverlayRuntimeState(
    const GeneratedValuesOverlayConfig& config)
    : max_fill_keys_(config.max_fill_keys) {
  claimed_ordinals_.reserve(config.sources.size());
  for (const auto& [source_id, source] : config.sources) {
    claimed_ordinals_.emplace(
        source_id, std::vector<uint8_t>(source.row_ordinals.size(), 0));
  }
}

void GeneratedValuesOverlayRuntimeState::claim(
    const std::vector<GeneratedValueClaim>& claims) {
  std::lock_guard<std::mutex> lock(mutex_);
  VELOX_USER_CHECK_LE(static_cast<uint64_t>(claims.size()),
                      max_fill_keys_ - std::min(max_fill_keys_, claimed_keys_),
                      "GeneratedValuesOverlay fill-key grant exceeded");
  for (const auto& claim : claims) {
    auto source = claimed_ordinals_.find(claim.source_object_id);
    VELOX_USER_CHECK(source != claimed_ordinals_.end() &&
                         claim.ordinal < source->second.size(),
                     "GeneratedValuesOverlay claim is outside its artifact");
    VELOX_USER_CHECK(
        source->second[claim.ordinal] == 0,
        "GeneratedValuesOverlay input contains a duplicate source row");
    source->second[claim.ordinal] = 1;
  }
  claimed_keys_ += claims.size();
}

GeneratedValuesOverlayOperator::GeneratedValuesOverlayOperator(
    int32_t operator_id, facebook::velox::exec::DriverCtx* driver_ctx,
    const std::shared_ptr<const GeneratedValuesOverlayNode>& node)
    : Operator(driver_ctx, node->outputType(), operator_id, node->id(),
               "GeneratedValuesOverlay"),
      config_(node->config()),
      runtime_(node->runtime()) {}

bool GeneratedValuesOverlayOperator::needsInput() const {
  return !noMoreInput_ && output_ == nullptr;
}

void GeneratedValuesOverlayOperator::addInput(
    facebook::velox::RowVectorPtr input) {
  VELOX_USER_CHECK(!noMoreInput_,
                   "GeneratedValuesOverlay received input after EOS");
  VELOX_USER_CHECK(
      output_ == nullptr,
      "GeneratedValuesOverlay received input before output was consumed");
  if (!input || input->size() == 0) {
    output_ = std::dynamic_pointer_cast<facebook::velox::RowVector>(
        facebook::velox::BaseVector::create(outputType_, 0, pool()));
    return;
  }
  facebook::velox::DecodedVector source_objects(
      *input->childAt(config_->source_object_channel));
  facebook::velox::DecodedVector row_positions(
      *input->childAt(config_->row_position_channel));

  uint32_t dimension = 0;
  std::vector<GeneratedValueClaim> claims;
  claims.reserve(input->size());
  for (facebook::velox::vector_size_t row = 0; row < input->size(); ++row) {
    VELOX_USER_CHECK(
        !source_objects.isNullAt(row) && !row_positions.isNullAt(row),
        "GeneratedValuesOverlay join keys must be non-null");
    const auto source =
        source_objects.valueAt<facebook::velox::StringView>(row);
    const std::string source_id(source.data(), source.size());
    const int64_t signed_position = row_positions.valueAt<int64_t>(row);
    VELOX_USER_CHECK_GE(
        signed_position, 0,
        "GeneratedValuesOverlay physical row position must not be negative");
    const uint64_t position = static_cast<uint64_t>(signed_position);
    auto source_values = config_->sources.find(source_id);
    VELOX_USER_CHECK(
        source_values != config_->sources.end(),
        "GeneratedValuesOverlay has no artifact for source object");
    auto ordinal = source_values->second.row_ordinals.find(position);
    VELOX_USER_CHECK(
        ordinal != source_values->second.row_ordinals.end(),
        "GeneratedValuesOverlay artifact is missing a physical row position");
    claims.push_back(GeneratedValueClaim{source_id, ordinal->second});
    if (dimension == 0) {
      dimension = source_values->second.dimension;
    }
    VELOX_USER_CHECK_EQ(
        source_values->second.dimension, dimension,
        "GeneratedValuesOverlay source objects have different dimensions");
  }
  runtime_->claim(claims);

  const auto rows = input->size();
  VELOX_USER_CHECK_LE(
      dimension,
      static_cast<uint64_t>(
          std::numeric_limits<facebook::velox::vector_size_t>::max()) /
          static_cast<uint64_t>(rows),
      "GeneratedValuesOverlay output element count exceeds Velox bounds");
  auto offsets =
      facebook::velox::AlignedBuffer::allocate<facebook::velox::vector_size_t>(
          rows, pool());
  auto sizes =
      facebook::velox::AlignedBuffer::allocate<facebook::velox::vector_size_t>(
          rows, pool());
  auto* raw_offsets = offsets->asMutable<facebook::velox::vector_size_t>();
  auto* raw_sizes = sizes->asMutable<facebook::velox::vector_size_t>();
  auto elements = facebook::velox::BaseVector::create(facebook::velox::REAL(),
                                                      rows * dimension, pool());
  auto* flat_elements = elements->asFlatVector<float>();
  auto nulls = facebook::velox::AlignedBuffer::allocate<bool>(
      rows, pool(), facebook::velox::bits::kNotNull);
  auto* raw_nulls = nulls->asMutable<uint64_t>();

  for (facebook::velox::vector_size_t row = 0; row < rows; ++row) {
    const auto source =
        source_objects.valueAt<facebook::velox::StringView>(row);
    const std::string source_id(source.data(), source.size());
    const uint64_t position =
        static_cast<uint64_t>(row_positions.valueAt<int64_t>(row));
    const auto& source_values = config_->sources.at(source_id);
    const uint64_t ordinal = source_values.row_ordinals.at(position);
    VELOX_USER_CHECK_LT(
        ordinal, source_values.validity.size(),
        "GeneratedValuesOverlay validity ordinal is out of bounds");
    const uint64_t base = ordinal * dimension;
    VELOX_USER_CHECK_LE(
        base + dimension, source_values.values.size(),
        "GeneratedValuesOverlay artifact ordinal is out of bounds");
    raw_offsets[row] = row * dimension;
    raw_sizes[row] = source_values.validity[ordinal] == 0 ? 0 : dimension;
    facebook::velox::bits::setNull(raw_nulls, row,
                                   source_values.validity[ordinal] == 0);
    for (uint32_t index = 0; index < dimension; ++index) {
      flat_elements->set(row * dimension + index,
                         source_values.values[base + index]);
    }
  }

  auto generated_values = std::make_shared<facebook::velox::ArrayVector>(
      pool(), outputType_->childAt(outputType_->size() - 1), nulls, rows,
      offsets, sizes, elements);
  auto children = input->children();
  children.push_back(std::move(generated_values));
  output_ = std::make_shared<facebook::velox::RowVector>(
      pool(), outputType_, nullptr, rows, std::move(children));
}

facebook::velox::RowVectorPtr GeneratedValuesOverlayOperator::getOutput() {
  return std::exchange(output_, nullptr);
}

void GeneratedValuesOverlayOperator::noMoreInput() { Operator::noMoreInput(); }

bool GeneratedValuesOverlayOperator::isFinished() {
  return noMoreInput_ && output_ == nullptr;
}

facebook::velox::exec::BlockingReason GeneratedValuesOverlayOperator::isBlocked(
    facebook::velox::ContinueFuture* /*future*/) {
  return facebook::velox::exec::BlockingReason::kNotBlocked;
}

std::unique_ptr<facebook::velox::exec::Operator>
GeneratedValuesOverlayTranslator::toOperator(
    facebook::velox::exec::DriverCtx* ctx, int32_t id,
    const facebook::velox::core::PlanNodePtr& node) {
  auto overlay =
      std::dynamic_pointer_cast<const GeneratedValuesOverlayNode>(node);
  if (!overlay) {
    return nullptr;
  }
  return std::make_unique<GeneratedValuesOverlayOperator>(id, ctx, overlay);
}

void registerGeneratedValuesOverlayOperator() {
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<GeneratedValuesOverlayTranslator>());
}

}  // namespace pangrova::velox
