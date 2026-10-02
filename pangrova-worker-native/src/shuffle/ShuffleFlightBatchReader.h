#pragma once

#include <arrow/array/array_binary.h>
#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "shuffle/FlightShuffleServer.h"

namespace pangrova::shuffle::detail {

// One indivisible serialized payload per Flight batch. The Arrow data buffer
// owns the popped batch (including its transit credit) until Flight has
// serialized and synchronously written that payload.
// The RPC context is borrowed only for the lifetime of its Flight stream.
class ShuffleFlightBatchReader : public arrow::RecordBatchReader {
 public:
  ShuffleFlightBatchReader(
      std::shared_ptr<PartitionDemuxer> demuxer, int32_t partition_id,
      const arrow::flight::ServerCallContext& context,
      std::shared_ptr<ShuffleBufferStore::Reservation> reservation)
      : demuxer_(std::move(demuxer)), partition_id_(partition_id), context_(context),
        reservation_(std::move(reservation)),
        schema_(arrow::schema({arrow::field("data", arrow::binary())})) {}

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
    *out = nullptr;
    if (closed_.load() || context_.is_cancelled()) {
      return arrow::Status::Cancelled("shuffle Flight reader cancelled");
    }
    ShuffleBatch batch;
    while (!demuxer_->pop(partition_id_, batch, std::chrono::milliseconds(50),
                         [this] { return closed_.load() || context_.is_cancelled(); })) {
      ARROW_RETURN_NOT_OK(demuxer_->status());
      if (closed_.load() || context_.is_cancelled()) {
        return arrow::Status::Cancelled("shuffle Flight reader cancelled");
      }
      if (demuxer_->isPartitionDrained(partition_id_)) {
        ARROW_RETURN_NOT_OK(demuxer_->status());
        if (!demuxer_->markPartitionReadComplete(partition_id_)) {
          return arrow::Status::Invalid(
              "shuffle partition did not reach a clean committed EOF");
        }
        return arrow::Status::OK();
      }
    }
    ARROW_RETURN_NOT_OK(demuxer_->status());
    // The producer's canonical queue normally supplies one contiguous IOBuf.
    // An externally supplied chain is coalesced only after transit credit is
    // held, rather than allocating a full-page uncharged temporary.
    if (batch.data->isChained()) batch.data->coalesce();
    auto owner = std::make_shared<ShuffleBatch>(std::move(batch));
    const auto length = static_cast<int32_t>(owner->data->length());
    ARROW_ASSIGN_OR_RAISE(auto offsets, arrow::AllocateBuffer(2 * sizeof(int32_t)));
    auto* values = reinterpret_cast<int32_t*>(offsets->mutable_data());
    values[0] = 0;
    values[1] = length;
    auto data = std::shared_ptr<arrow::Buffer>(
        new arrow::Buffer(owner->data->data(), length),
        [owner](arrow::Buffer* buffer) { delete buffer; });
    auto array = std::make_shared<arrow::BinaryArray>(
        1, std::shared_ptr<arrow::Buffer>(std::move(offsets)), data, nullptr, 0);
    *out = arrow::RecordBatch::Make(schema_, 1, {array});
    return arrow::Status::OK();
  }

  arrow::Status Close() override {
    // Abandoning one partition must not cancel other partitions or block their
    // producer. Unread data remains within shared spill limits and linger TTL.
    closed_.store(true);
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<PartitionDemuxer> demuxer_;
  const int32_t partition_id_;
  const arrow::flight::ServerCallContext& context_;
  std::shared_ptr<ShuffleBufferStore::Reservation> reservation_;
  const std::shared_ptr<arrow::Schema> schema_;
  std::atomic<bool> closed_{false};
};

}  // namespace pangrova::shuffle::detail
