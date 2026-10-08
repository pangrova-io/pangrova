#include "result/ResultBatchStreamReader.h"
#include <arrow/util/key_value_metadata.h>

#include <chrono>
#include <algorithm>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <folly/io/IOBuf.h>
#include <velox/exec/OutputBufferManager.h>
#include "columnar/CanonicalBatchStream.h"
#include "columnar/CanonicalArrowValidation.h"
#include "capability/LogicalTypeContract.h"
#include "result/ResultReadAuthority.h"
#include "result/ResultPendingRead.h"
#include "observability/PangrovaLog.h"

namespace pangrova::result {
namespace {
int64_t nowMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> copyPage(const uint8_t* data, int64_t length,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool) {
  if (!pool || !data || length <= 0 || length > columnar::kMaximumBatchStreamBytes) {
    return arrow::Status::Invalid("Result stream has invalid ownership or wire size");
  }
  ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(length, pool.get()));
  std::memcpy(buffer->mutable_data(), data, static_cast<size_t>(length));
  return memory::retainColumnarBuffer(std::shared_ptr<arrow::Buffer>(std::move(buffer)), pool);
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> decodePage(
    std::shared_ptr<arrow::Buffer> buffer,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool) {
  ARROW_ASSIGN_OR_RAISE(auto batches, columnar::decodeBatchStream(std::move(buffer), pool));
  for (const auto& batch : batches) {
    if (!schema) return arrow::Status::Invalid("Result wire schema is unavailable");
    ARROW_RETURN_NOT_OK(columnar::validateCanonicalArrowBatch(*batch, *schema));
  }
  return batches;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ownPage(std::unique_ptr<folly::IOBuf> page,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool) {
  if (page->isChained()) {
    const auto length = page->computeChainDataLength();
    if (length > static_cast<uint64_t>(columnar::kMaximumBatchStreamBytes)) {
      return arrow::Status::CapacityError("Result page exceeds wire ceiling");
    }
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(static_cast<int64_t>(length), pool.get()));
    auto* target = buffer->mutable_data();
    for (const auto& range : *page) {
      std::memcpy(target, range.data(), range.size());
      target += range.size();
    }
    return memory::retainColumnarBuffer(std::shared_ptr<arrow::Buffer>(std::move(buffer)), pool);
  }
  struct Owner {
    std::shared_ptr<memory::ColumnarMemoryPool> pool;
    std::unique_ptr<folly::IOBuf> page;
    arrow::Buffer buffer;
    Owner(std::unique_ptr<folly::IOBuf> value, std::shared_ptr<memory::ColumnarMemoryPool> allocator)
        : pool(std::move(allocator)), page(std::move(value)), buffer(page->data(), page->length()) {}
  };
  auto owner = std::make_shared<Owner>(std::move(page), pool);
  return std::shared_ptr<arrow::Buffer>(owner, &owner->buffer);
}

struct ReaderLease {
  std::shared_ptr<ResultOwnershipState> state;
  explicit ReaderLease(std::shared_ptr<ResultOwnershipState> value) : state(std::move(value)) {
    state->acquireReader();
  }
  ~ReaderLease() { state->releaseReader(); }
};

class ResultBatchReader final : public arrow::RecordBatchReader {
 public:
  ResultBatchReader(FlightResultServer::TaskOutputInfo info, protocol::v2::ResultReadGrant grant,
      std::shared_ptr<arrow::Schema> storage, std::shared_ptr<arrow::Schema> result,
      int64_t maximumBytes, int64_t timeoutSeconds,
      const arrow::flight::ServerCallContext* context)
      : info_(std::move(info)), lease_(std::make_shared<ReaderLease>(info_.ownership)),
        grant_(std::move(grant)), storage_(std::move(storage)),
        result_(std::move(result)), maximumBytes_(maximumBytes), timeoutSeconds_(timeoutSeconds),
        context_(context) {}

  ~ResultBatchReader() override {
    info_.ownership->revokeAuthority();
    try {
      info_.buffers->deleteResults(taskId(), 0);
      info_.ownership->revokeAndClearQueue();
    } catch (const std::exception& error) {
      // A destructor cannot throw across Arrow/gRPC. Do not claim queue drain
      // on failure; the retained task owner must retry the actual cleanup.
      PANGROVA_LOG(ERROR) << "event=result_reader_cleanup_failed task_id=" << taskId()
                         << " error=" << error.what();
    }
  }

  std::shared_ptr<arrow::Schema> schema() const override { return result_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
    *out = nullptr;
    if (!failure_.ok()) return failure_;
    ARROW_RETURN_NOT_OK(validateLiveAuthority());
    while (pending_.empty() && !finished_) {
      const auto status = fetchPages();
      if (!status.ok()) {
        failure_ = status;
        return failure_;
      }
    }
    if (!pending_.empty()) {
      *out = std::move(pending_.front());
      pending_.pop_front();
    }
    return arrow::Status::OK();
  }

 private:
  const std::string& taskId() const { return grant_.attempt().task_id(); }

  arrow::Status validateLiveAuthority() const {
    if ((context_ && context_->is_cancelled()) || info_.ownership->evidence().authority_revoked ||
        grant_.expires_at_epoch_ms() <= static_cast<uint64_t>(nowMillis())) {
      return arrow::Status::Cancelled("Result authority was revoked or expired");
    }
    return validateResultRead(grant_, info_.authority);
  }

  arrow::Result<ResultPages> fetch() {
    const int64_t deadlineMillis = static_cast<int64_t>(grant_.expires_at_epoch_ms()) - nowMillis();
    const auto waitMillis = timeoutSeconds_ > 0
        ? std::min(deadlineMillis, timeoutSeconds_ * 1000) : deadlineMillis;
    if (waitMillis <= 0) return arrow::Status::Cancelled("Result authority expired");
    auto pending = std::make_shared<ResultPendingRead>();
    ARROW_RETURN_NOT_OK(info_.ownership->attachPendingRead(pending));
    struct Subscription {
      std::shared_ptr<ResultOwnershipState> ownership;
      std::shared_ptr<ResultPendingRead> pending;
      ~Subscription() { ownership->detachPendingRead(pending); }
    } subscription{info_.ownership, pending};
    try {
      // SDK Task::terminate can erase the route without notifying getData.
      // Revocation wakes this exact subscription even after that route is gone.
      const bool found = info_.buffers->getData(taskId(), 0,
          static_cast<uint64_t>(maximumBytes_), sequence_,
          [pending](std::vector<std::unique_ptr<folly::IOBuf>> pages, int64_t sequence,
              std::vector<int64_t>) { pending->complete(ResultPages{std::move(pages), sequence}); });
      if (!found) pending->cancel(arrow::Status::IOError("Result producer disappeared before its committed EOF"));
      return pending->waitUntil(std::chrono::steady_clock::now() + std::chrono::milliseconds(waitMillis), context_);
    } catch (const std::exception& error) {
      return arrow::Status::IOError("Result producer failed before its committed EOF: ", error.what());
    }
  }

  arrow::Status fetchPages() {
    ARROW_ASSIGN_OR_RAISE(auto fetched, fetch());
    if (fetched.sequence != sequence_ || fetched.pages.empty()) {
      return arrow::Status::SerializationError("Result sequence changed or EOF evidence is absent");
    }
    std::deque<std::shared_ptr<arrow::RecordBatch>> staged;
    int64_t nextSequence = sequence_;
    bool nextFinished = false;
    for (size_t i = 0; i < fetched.pages.size(); ++i) {
      auto& page = fetched.pages[i];
      if (!page) {
        if (i + 1 != fetched.pages.size()) return arrow::Status::SerializationError("Result data follows EOF");
        nextFinished = true;
        break;
      }
      ARROW_ASSIGN_OR_RAISE(auto buffer, ownPage(std::move(page), info_.pool));
      ARROW_ASSIGN_OR_RAISE(auto batches, decodePage(std::move(buffer), storage_, info_.pool));
      for (const auto& batch : batches) {
        for (const auto& column : info_.result_column_types) {
          if (column.column_index >= static_cast<uint32_t>(batch->num_columns())) {
            return arrow::Status::Invalid("VECTOR result column is outside its batch schema");
          }
          try {
            capability::validateVectorResultArray(column.logical_type, *batch->column(column.column_index));
          } catch (const std::invalid_argument& error) {
            return arrow::Status::Invalid("Invalid VECTOR result values: ", error.what());
          }
        }
        staged.push_back(retainResultBatch(arrow::RecordBatch::Make(
            result_, batch->num_rows(), batch->columns()), info_.ownership));
      }
      ++nextSequence;
    }
    // Fetching and decoding can outlive the authority checked at ReadNext entry.
    ARROW_RETURN_NOT_OK(validateLiveAuthority());
    // No page, cursor, EOF or ACK is committed until the entire fetched group
    // has passed wire-schema and required-value validation.
    pending_.swap(staged);
    sequence_ = nextSequence;
    finished_ = nextFinished;
    info_.buffers->acknowledge(taskId(), 0, sequence_);
    if (finished_) info_.buffers->deleteResults(taskId(), 0);
    return arrow::Status::OK();
  }

  FlightResultServer::TaskOutputInfo info_;
  std::shared_ptr<ReaderLease> lease_;
  protocol::v2::ResultReadGrant grant_;
  std::shared_ptr<arrow::Schema> storage_, result_;
  int64_t maximumBytes_, timeoutSeconds_, sequence_ = 0;
  bool finished_ = false;
  arrow::Status failure_ = arrow::Status::OK();
  std::deque<std::shared_ptr<arrow::RecordBatch>> pending_;
  // Arrow retains ServerCallContext for the full synchronous DoGet transport,
  // including streaming and destruction of its RecordBatchStream.
  const arrow::flight::ServerCallContext* context_;
};
}  // namespace

namespace detail {
arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> parseArrowBatchFrames(
    const std::shared_ptr<arrow::Schema>& schema, const uint8_t* data, int64_t length,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool) {
  ARROW_ASSIGN_OR_RAISE(auto copied, copyPage(data, length, pool));
  return decodePage(std::move(copied), schema, pool);
}
}  // namespace detail

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> resultBatchStreamReader(
    FlightResultServer::TaskOutputInfo info, protocol::v2::ResultReadGrant grant,
    int64_t maximumResponseBytes, int64_t timeoutSeconds,
    const arrow::flight::ServerCallContext* context) {
  if (!info.pool || !info.buffers || !info.output_type || !info.wire_schema || !info.ownership || maximumResponseBytes <= 0 ||
      maximumResponseBytes > columnar::kMaximumBatchStreamBytes ||
      static_cast<size_t>(info.wire_schema->num_fields()) != info.output_type->size() ||
      info.column_labels.size() != info.output_type->size()) {
    return arrow::Status::Invalid("Result registration lacks its allocator, schema or exact ordinal labels");
  }
  auto storage = info.wire_schema;
  ARROW_ASSIGN_OR_RAISE(auto typed, detail::withResultColumnTypes(storage, info.result_column_types));
  auto fields = typed->fields();
  for (size_t i = 0; i < fields.size(); ++i) {
    auto metadata = fields[i]->metadata() ? fields[i]->metadata()->Copy() : std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append("pangrova:result:ordinal", std::to_string(i));
    fields[i] = fields[i]->WithMetadata(metadata)->WithName(info.column_labels[i]);
  }
  auto result = arrow::schema(std::move(fields), typed->metadata());
  const auto ownership = info.ownership;
  try {
    return std::shared_ptr<arrow::RecordBatchReader>(std::make_shared<ResultBatchReader>(
        std::move(info), std::move(grant), std::move(storage), std::move(result),
        maximumResponseBytes, timeoutSeconds, context));
  } catch (const std::runtime_error& error) {
    if (ownership->evidence().authority_revoked) {
      return arrow::Status::Cancelled("Result reader admission failed: ", error.what());
    }
    return arrow::Status::Invalid("Result reader admission failed: ", error.what());
  }
}
}  // namespace pangrova::result
