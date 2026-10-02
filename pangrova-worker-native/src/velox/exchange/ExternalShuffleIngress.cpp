#include "velox/exchange/ExternalShuffleIngress.h"

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>
#include <utility>

#include <openssl/crypto.h>
#include <openssl/evp.h>

namespace pangrova::velox {

ExternalShuffleIngress::ExternalShuffleIngress(
    size_t normal_buffer_bytes,
    size_t max_batch_bytes,
    uint64_t deadline_epoch_ms)
    : normal_buffer_bytes_(normal_buffer_bytes),
      max_batch_bytes_(max_batch_bytes),
      deadline_epoch_ms_(deadline_epoch_ms) {
  if (normal_buffer_bytes_ == 0 || max_batch_bytes_ == 0 ||
      normal_buffer_bytes_ > max_batch_bytes_ || deadline_epoch_ms_ == 0) {
    throw std::invalid_argument(
        "external shuffle ingress requires valid buffer, batch, and deadline bounds");
  }
}

void ExternalShuffleIngress::push(ExternalShuffleChunk chunk) {
  if (chunk.batch_sequence < 0 || chunk.chunk_count == 0 ||
      chunk.chunk_index >= chunk.chunk_count || chunk.data.empty() ||
      chunk.data.size() > kExternalShuffleChunkBytes ||
      chunk.batch_size_bytes == 0 ||
      chunk.batch_size_bytes > max_batch_bytes_ ||
      chunk.batch_size_bytes > std::numeric_limits<size_t>::max() ||
      chunk.batch_checksum.size() != 32) {
    throw std::invalid_argument(
        "external shuffle chunk violates the canonical bounds");
  }
  const auto batch_size = static_cast<size_t>(chunk.batch_size_bytes);
  const auto expected_chunk_count =
      1 + (batch_size - 1) / kExternalShuffleChunkBytes;
  if (expected_chunk_count != chunk.chunk_count) {
    throw std::invalid_argument(
        "external shuffle chunk count does not match the batch size");
  }
  const auto chunk_offset =
      static_cast<size_t>(chunk.chunk_index) * kExternalShuffleChunkBytes;
  const auto expected_chunk_size =
      std::min(kExternalShuffleChunkBytes, batch_size - chunk_offset);
  if (chunk.data.size() != expected_chunk_size) {
    throw std::invalid_argument(
        "external shuffle chunk size does not match the canonical layout");
  }

  std::unique_lock<std::mutex> lock(mutex_);
  if (chunk.batch_sequence != next_sequence_) {
    throw std::logic_error("external shuffle batch sequence is not contiguous");
  }
  const auto deadline = std::chrono::system_clock::time_point(
      std::chrono::milliseconds(deadline_epoch_ms_));
  if (chunk.chunk_index == 0) {
    if (assembly_.has_value()) {
      throw std::logic_error("external shuffle batch assembly already exists");
    }
    not_full_.wait_until(lock, deadline, [this, batch_size] {
      const bool normal_capacity =
          batch_size <= normal_buffer_bytes_ &&
          buffered_bytes_ <= normal_buffer_bytes_ - batch_size;
      const bool exclusive_capacity =
          batch_size > normal_buffer_bytes_ && buffered_bytes_ == 0;
      return cancelled_ || finished_ || !failure_.empty() ||
          deadlineExpired() || normal_capacity || exclusive_capacity;
    });
  }
  if (cancelled_) {
    throw std::runtime_error("external shuffle ingress is cancelled");
  }
  if (finished_ || !failure_.empty()) {
    throw std::logic_error("external shuffle ingress is already terminal");
  }
  if (deadlineExpired()) {
    discardAssemblyLocked();
    lock.unlock();
    not_full_.notify_all();
    throw std::runtime_error("external shuffle ingress deadline expired");
  }

  if (chunk.chunk_index == 0) {
    Assembly assembly;
    assembly.batch_sequence = chunk.batch_sequence;
    assembly.chunk_count = chunk.chunk_count;
    assembly.batch_size_bytes = static_cast<size_t>(chunk.batch_size_bytes);
    assembly.batch_checksum = chunk.batch_checksum;
    assembly.data.reserve(assembly.batch_size_bytes);
    buffered_bytes_ += assembly.batch_size_bytes;
    assembly_ = std::move(assembly);
  }
  if (!assembly_.has_value()) {
    throw std::logic_error("external shuffle chunk has no active batch");
  }
  if (
      assembly_->batch_sequence != chunk.batch_sequence ||
      assembly_->next_chunk_index != chunk.chunk_index ||
      assembly_->chunk_count != chunk.chunk_count ||
      assembly_->batch_size_bytes != chunk.batch_size_bytes ||
      CRYPTO_memcmp(
          assembly_->batch_checksum.data(),
          chunk.batch_checksum.data(),
          chunk.batch_checksum.size()) != 0) {
    discardAssemblyLocked();
    not_full_.notify_all();
    throw std::logic_error("external shuffle chunk metadata changed mid-batch");
  }
  if (chunk.data.size() >
      assembly_->batch_size_bytes - assembly_->data.size()) {
    discardAssemblyLocked();
    not_full_.notify_all();
    throw std::logic_error("external shuffle chunks exceed declared batch size");
  }
  assembly_->data.append(chunk.data);
  ++assembly_->next_chunk_index;
  if (assembly_->next_chunk_index == assembly_->chunk_count) {
    if (assembly_->data.size() != assembly_->batch_size_bytes) {
      discardAssemblyLocked();
      not_full_.notify_all();
      throw std::logic_error("external shuffle batch size does not match chunks");
    }
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int digest_size = 0;
    if (EVP_Digest(
            assembly_->data.data(), assembly_->data.size(), digest.data(),
            &digest_size, EVP_sha256(), nullptr) != 1 ||
        digest_size != 32 ||
        CRYPTO_memcmp(
            digest.data(), assembly_->batch_checksum.data(), digest_size) != 0) {
      discardAssemblyLocked();
      not_full_.notify_all();
      throw ExternalShuffleChecksumError(
          "external shuffle reassembled batch checksum mismatch");
    }
    batches_.push_back(std::move(assembly_->data));
    assembly_.reset();
    ++next_sequence_;
  }
  lock.unlock();
  not_empty_.notify_one();
}

void ExternalShuffleIngress::finish(int64_t last_sequence) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (cancelled_ || finished_ || !failure_.empty()) {
    throw std::logic_error("external shuffle ingress has duplicate terminal state");
  }
  if (assembly_.has_value()) {
    discardAssemblyLocked();
    not_full_.notify_all();
    throw std::logic_error("external shuffle terminal arrived mid-batch");
  }
  if (last_sequence != next_sequence_ - 1) {
    throw std::logic_error("external shuffle terminal sequence mismatch");
  }
  finished_ = true;
  not_empty_.notify_all();
  not_full_.notify_all();
}

void ExternalShuffleIngress::fail(std::string message) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (cancelled_ || finished_ || !failure_.empty() || message.empty()) {
    throw std::logic_error("invalid external shuffle failure transition");
  }
  failure_ = std::move(message);
  batches_.clear();
  assembly_.reset();
  buffered_bytes_ = 0;
  not_empty_.notify_all();
  not_full_.notify_all();
}

void ExternalShuffleIngress::cancel() {
  std::lock_guard<std::mutex> lock(mutex_);
  cancelled_ = true;
  batches_.clear();
  assembly_.reset();
  buffered_bytes_ = 0;
  not_empty_.notify_all();
  not_full_.notify_all();
}

ExternalShuffleIngress::Result ExternalShuffleIngress::take(
    std::chrono::microseconds max_wait) {
  std::unique_lock<std::mutex> lock(mutex_);
  const auto ready = [this] {
    return cancelled_ || !failure_.empty() || !batches_.empty() ||
        finished_ || deadlineExpired();
  };
  const auto deadline = std::chrono::system_clock::time_point(
      std::chrono::milliseconds(deadline_epoch_ms_));
  const auto requested = max_wait.count() <= 0
      ? deadline
      : std::min(
          deadline, std::chrono::system_clock::now() +
              std::chrono::duration_cast<std::chrono::system_clock::duration>(
                  max_wait));
  if (!not_empty_.wait_until(lock, requested, ready) && !deadlineExpired()) {
    return {ResultKind::kPending, {}, {}};
  }
  if (cancelled_) {
    return {ResultKind::kCancelled, {}, {}};
  }
  if (!failure_.empty()) {
    return {ResultKind::kFailed, {}, failure_};
  }
  if (deadlineExpired()) {
    batches_.clear();
    assembly_.reset();
    buffered_bytes_ = 0;
    lock.unlock();
    not_full_.notify_all();
    return {ResultKind::kFailed, {}, "external shuffle ingress deadline expired"};
  }
  if (!batches_.empty()) {
    auto data = std::move(batches_.front());
    batches_.pop_front();
    buffered_bytes_ -= data.size();
    lock.unlock();
    not_full_.notify_one();
    return {ResultKind::kData, std::move(data), {}};
  }
  return {ResultKind::kFinished, {}, {}};
}

bool ExternalShuffleIngress::deadlineExpired() const {
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  return now >= 0 && static_cast<uint64_t>(now) >= deadline_epoch_ms_;
}

void ExternalShuffleIngress::discardAssemblyLocked() {
  if (!assembly_.has_value()) {
    return;
  }
  buffered_bytes_ -= assembly_->batch_size_bytes;
  assembly_.reset();
}

}  // namespace pangrova::velox
