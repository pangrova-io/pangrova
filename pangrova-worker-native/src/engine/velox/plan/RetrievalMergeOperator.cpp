#include "engine/velox/plan/RetrievalMergeOperator.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/SelectivityVector.h"

namespace pangrova::velox {
namespace {

void appendLength(std::string& output, size_t length) {
  uint64_t value = static_cast<uint64_t>(length);
  // Stable keys participate in the final score tie-break.  Never append the
  // host representation here: a little-endian producer and a big-endian
  // producer must derive the same ordering for the same candidate rows.
  for (int shift = 56; shift >= 0; shift -= 8) {
    output.push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

}  // namespace

RetrievalMergeOperator::RetrievalMergeOperator(
    int32_t operator_id,
    facebook::velox::exec::DriverCtx* driver_ctx,
    const std::shared_ptr<const RetrievalMergeNode>& node)
    : Operator(
          driver_ctx,
          node->outputType(),
          operator_id,
          node->id(),
          "RetrievalMerge"),
      config_(node->config()) {}

bool RetrievalMergeOperator::needsInput() const {
  return !noMoreInput_;
}

void RetrievalMergeOperator::addInput(facebook::velox::RowVectorPtr input) {
  VELOX_USER_CHECK(!noMoreInput_, "RetrievalMerge received input after EOS");
  if (!input || input->size() == 0) {
    return;
  }
  uint64_t rows = static_cast<uint64_t>(input->size());
  VELOX_USER_CHECK_LE(
      rows,
      config_.max_candidate_rows -
          std::min<uint64_t>(config_.max_candidate_rows, candidates_.size()),
      "RetrievalMerge candidate row limit exceeded");
  uint64_t batch_bytes = input->estimateFlatSize();
  VELOX_USER_CHECK_LE(
      batch_bytes,
      config_.max_candidate_bytes -
          std::min(config_.max_candidate_bytes, candidate_bytes_),
      "RetrievalMerge candidate byte limit exceeded");

  facebook::velox::LazyVector::ensureLoadedRows(
      input, facebook::velox::SelectivityVector(input->size()));
  facebook::velox::DecodedVector scores(*input->childAt(config_.score_channel));
  facebook::velox::DecodedVector sources(
      *input->childAt(config_.source_id_channel));

  size_t batch_index = input_batches_.size();
  for (facebook::velox::vector_size_t row = 0; row < input->size(); ++row) {
    VELOX_USER_CHECK(
        !scores.isNullAt(row) && !sources.isNullAt(row),
        "RetrievalMerge score and source_id must be non-null");
    double score = scores.valueAt<double>(row);
    auto source_view = sources.valueAt<facebook::velox::StringView>(row);
    VELOX_USER_CHECK(
        std::isfinite(score) && !source_view.empty(),
        "RetrievalMerge candidate value violates the canonical contract");
    size_t ordinal = row_references_.size();
    row_references_.push_back(RowReference{batch_index, row});
    candidates_.push_back(pangrova::retrieval::Candidate{
        encodeStableKey(input, row),
        std::string(source_view.data(), source_view.size()),
        score,
        ordinal});
  }
  candidate_bytes_ += batch_bytes;
  input_batches_.push_back(std::move(input));
}

std::string RetrievalMergeOperator::encodeStableKey(
    const facebook::velox::RowVectorPtr& input,
    facebook::velox::vector_size_t row) const {
  std::string result;
  if (config_.physical_identity) {
    VELOX_USER_CHECK_EQ(config_.stable_key_channels.size(), 4,
                       "Physical retrieval identity requires four channels");
    for (size_t ordinal = 0; ordinal < 4; ++ordinal) {
      facebook::velox::DecodedVector value(*input->childAt(config_.stable_key_channels[ordinal]));
      VELOX_USER_CHECK(!value.isNullAt(row), "Physical retrieval identity must be non-null");
      if (ordinal == 3) {
        const int64_t position = value.valueAt<int64_t>(row);
        VELOX_USER_CHECK_GE(position, 0, "Physical retrieval position must be nonnegative");
        appendLength(result, static_cast<size_t>(position));
      } else {
        const auto text = value.valueAt<facebook::velox::StringView>(row);
        VELOX_USER_CHECK(!text.empty(), "Physical retrieval authority must be nonempty");
        appendLength(result, text.size());
        result.append(text.data(), text.size());
      }
    }
    return result;
  }
  for (size_t channel : config_.stable_key_channels) {
    const auto& value = input->childAt(channel);
    if (value->isNullAt(row)) {
      result.push_back('\0');
      continue;
    }
    result.push_back('\1');
    std::string encoded = value->toString(row);
    appendLength(result, encoded.size());
    result.append(encoded);
  }
  return result;
}

void RetrievalMergeOperator::noMoreInput() {
  Operator::noMoreInput();
  materializeFusion();
}

void RetrievalMergeOperator::materializeFusion() {
  if (fusion_materialized_) {
    return;
  }
  if (config_.kind == RetrievalMergeKind::kRrf) {
    fused_ = pangrova::retrieval::RetrievalFusion::rrf(
        candidates_, config_.sources, config_.rrf_k,
        static_cast<size_t>(config_.output_limit),
        static_cast<size_t>(config_.max_candidate_rows),
        static_cast<size_t>(config_.max_fan_in));
  } else {
    fused_ = pangrova::retrieval::RetrievalFusion::linear(
        candidates_, config_.sources,
        static_cast<size_t>(config_.output_limit),
        static_cast<size_t>(config_.max_candidate_rows),
        static_cast<size_t>(config_.max_fan_in));
  }
  candidates_.clear();
  candidates_.shrink_to_fit();
  fusion_materialized_ = true;
}

facebook::velox::RowVectorPtr RetrievalMergeOperator::getOutput() {
  if (!noMoreInput_ || output_offset_ >= fused_.size()) {
    return nullptr;
  }
  size_t count = std::min(
      fused_.size() - output_offset_,
      static_cast<size_t>(outputBatchRows()));
  auto output = buildOutputBatch(output_offset_, count);
  output_offset_ += count;
  if (output_offset_ == fused_.size()) {
    input_batches_.clear();
    row_references_.clear();
  }
  return output;
}

facebook::velox::RowVectorPtr RetrievalMergeOperator::buildOutputBatch(
    size_t begin,
    size_t count) {
  auto output_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      outputType_);
  auto output_size = static_cast<facebook::velox::vector_size_t>(count);
  std::vector<facebook::velox::VectorPtr> children;
  children.reserve(output_type->size());
  for (size_t channel = 0; channel < output_type->size(); ++channel) {
    children.push_back(facebook::velox::BaseVector::create(
        output_type->childAt(channel), output_size, operatorCtx_->pool()));
  }

  for (size_t index = 0; index < count; ++index) {
    const auto& fused = fused_[begin + index];
    VELOX_USER_CHECK_LT(
        fused.representative_ordinal,
        row_references_.size(),
        "RetrievalMerge representative row is invalid");
    const auto& reference = row_references_[fused.representative_ordinal];
    const auto& input = input_batches_[reference.batch];
    auto output_row = static_cast<facebook::velox::vector_size_t>(index);
    for (size_t channel = 0; channel < children.size(); ++channel) {
      children[channel]->copy(
          input->childAt(channel).get(), output_row, reference.row, 1);
    }
    auto* score_vector = children[config_.score_channel]->asFlatVector<double>();
    score_vector->set(output_row, fused.fused_score);
  }
  return std::make_shared<facebook::velox::RowVector>(
      operatorCtx_->pool(), output_type, facebook::velox::BufferPtr(nullptr),
      output_size, std::move(children));
}

bool RetrievalMergeOperator::isFinished() {
  return noMoreInput_ && fusion_materialized_ && output_offset_ >= fused_.size();
}

facebook::velox::exec::BlockingReason RetrievalMergeOperator::isBlocked(
    facebook::velox::ContinueFuture* /*future*/) {
  return facebook::velox::exec::BlockingReason::kNotBlocked;
}

std::unique_ptr<facebook::velox::exec::Operator>
RetrievalMergeTranslator::toOperator(
    facebook::velox::exec::DriverCtx* ctx,
    int32_t id,
    const facebook::velox::core::PlanNodePtr& node) {
  auto merge = std::dynamic_pointer_cast<const RetrievalMergeNode>(node);
  if (!merge) {
    return nullptr;
  }
  return std::make_unique<RetrievalMergeOperator>(id, ctx, merge);
}

void registerRetrievalMergeOperator() {
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<RetrievalMergeTranslator>());
}

}  // namespace pangrova::velox
