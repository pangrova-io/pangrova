// FlightResultServer: Arrow Flight server that pulls pre-serialized
// SerializedPage from Velox's OutputBufferManager and serves them
// to the coordinator via Arrow Flight DoGet.
//
// Aligned with Prestissimo: same Velox components, Arrow Flight transport.

#include "result/FlightResultServer.h"

#include <chrono>
#include <cstring>
#include <deque>
#include <fstream>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>
#include <velox/exec/OutputBufferManager.h>
#include <velox/exec/Task.h>
#include <velox/common/memory/Memory.h>
#include <velox/common/memory/ByteStream.h>
#include <velox/serializers/PrestoSerializer.h>
#include <velox/common/base/VeloxException.h>
#include <velox/vector/arrow/Bridge.h>

#include "jni/bridge/ColumnarDataExporter.h"
#include "observability/PangrovaLog.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "velox/runtime/NativeEngineLifecycle.h"

namespace pangrova::result {

// F-NATIVE-SHUF-08: Singleton instance pointer for per-instance caches.
std::atomic<FlightResultServer*> FlightResultServer::instance_{nullptr};

FlightResultServer* FlightResultServer::instance() {
  return instance_.load(std::memory_order_acquire);
}

void FlightResultServer::registerTaskOutputType(
    const std::string& task_id,
    facebook::velox::RowTypePtr output_type) {
  auto* inst = instance();
  if (!inst) return;
  std::lock_guard<std::mutex> lock(inst->output_type_mutex_);
  inst->output_type_cache_[task_id] = std::move(output_type);
}

void FlightResultServer::unregisterTaskOutputType(const std::string& task_id) {
  auto* inst = instance();
  if (!inst) return;
  std::lock_guard<std::mutex> lock(inst->output_type_mutex_);
  inst->output_type_cache_.erase(task_id);
}

void FlightResultServer::registerTaskSerdeMode(
    const std::string& task_id,
    const std::string& serde_mode) {
  auto* inst = instance();
  if (!inst) return;
  std::lock_guard<std::mutex> lock(inst->serde_mode_mutex_);
  inst->serde_mode_cache_[task_id] = serde_mode;
}

void FlightResultServer::unregisterTaskSerdeMode(const std::string& task_id) {
  auto* inst = instance();
  if (!inst) return;
  std::lock_guard<std::mutex> lock(inst->serde_mode_mutex_);
  inst->serde_mode_cache_.erase(task_id);
}

std::string FlightResultServer::lookupTaskSerdeMode(
    const std::string& task_id) {
  auto* inst = instance();
  if (!inst) return "presto";
  std::lock_guard<std::mutex> lock(inst->serde_mode_mutex_);
  auto it = inst->serde_mode_cache_.find(task_id);
  if (it != inst->serde_mode_cache_.end()) {
    return it->second;
  }
  return "presto";
}

facebook::velox::RowTypePtr FlightResultServer::lookupTaskOutputType(
    const std::string& task_id) {
  auto* inst = instance();
  // Check instance cache first (populated by VeloxTaskRunner at task creation).
  if (inst) {
    std::lock_guard<std::mutex> lock(inst->output_type_mutex_);
    auto it = inst->output_type_cache_.find(task_id);
    if (it != inst->output_type_cache_.end()) {
      return it->second;
    }
  }
  // Fallback: check running tasks (may miss completed tasks).
  auto tasks = facebook::velox::exec::Task::getRunningTasks();
  for (auto& task : tasks) {
    if (task && task->taskId() == task_id) {
      auto type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
          task->planFragment().planNode->outputType());
      if (type && inst) {
        // Cache for future lookups.
        std::lock_guard<std::mutex> lock(inst->output_type_mutex_);
        inst->output_type_cache_[task_id] = type;
      }
      return type;
    }
  }
  return nullptr;
}

// ---------------------------------------------------------------------------
// detail:: implementations (F-NATIVE-SHUF-01 owning-buffer fix).
// ---------------------------------------------------------------------------
namespace detail {

FetchWaitDecision decideFetchWait(
    bool alive,
    int64_t elapsed_seconds,
    int64_t timeout_seconds) {
  if (!alive) {
    return FetchWaitDecision::kProducerDead;
  }
  if (timeout_seconds > 0 && elapsed_seconds >= timeout_seconds) {
    return FetchWaitDecision::kTimedOut;
  }
  return FetchWaitDecision::kKeepWaiting;
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
parseArrowBatchFrames(
    const std::shared_ptr<arrow::Schema>& schema,
    std::shared_ptr<arrow::Buffer> parent_buffer,
    const std::string& task_id) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  const uint8_t* data = parent_buffer->data();
  int64_t length = parent_buffer->size();
  int64_t offset = 0;
  while (offset + 4 <= length) {
    int32_t batch_length = 0;
    std::memcpy(&batch_length, data + offset, sizeof(int32_t));
    offset += sizeof(int32_t);

    if (batch_length <= 0 || offset + batch_length > length) {
      return arrow::Status::SerializationError(
          "Invalid batch frame in page: length=" +
          std::to_string(batch_length) + " offset=" +
          std::to_string(offset) + " page_length=" +
          std::to_string(length) + " task_id=" + task_id);
    }

    // PERF-T14: Use SliceBuffer to reference the parent buffer directly,
    // eliminating per-frame AllocateBuffer + memcpy. The parent_buffer's
    // lifetime is extended through shared_ptr reference counting.
    auto slice = arrow::SliceBuffer(parent_buffer, offset, batch_length);
    offset += batch_length;

    // Each frame is an IPC stream (schema + batch) produced by
    // ArrowBatchVectorSerde::writeBatchToStream. Use RecordBatchStreamReader
    // to parse the self-describing stream.
    auto buf_reader = std::make_shared<arrow::io::BufferReader>(slice);
    ARROW_ASSIGN_OR_RAISE(auto reader,
        arrow::ipc::RecordBatchStreamReader::Open(buf_reader));
    std::shared_ptr<arrow::RecordBatch> batch;
    ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
    if (batch) {
      batches.push_back(std::move(batch));
    }
  }
  return batches;
}

// F-NATIVE-BUILD-001: overload declared in FlightResultServer.h taking a raw
// (data, length) view. The batches produced by the buffer overload above slice
// their parent buffer zero-copy and keep it alive via shared_ptr refcounting, so
// to let the parsed batches outlive caller-owned/transient source memory (see
// DirectBatchStreamReaderLifetimeTest: BatchesSurviveSourceFree / SourcePoisoning)
// we copy the bytes into an Arrow-owned buffer and delegate to the buffer overload.
arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
parseArrowBatchFrames(
    const std::shared_ptr<arrow::Schema>& schema,
    const uint8_t* data,
    int64_t length,
    const std::string& task_id) {
  if (length < 0) {
    return arrow::Status::Invalid(
        "parseArrowBatchFrames: negative length=" + std::to_string(length) +
        " task_id=" + task_id);
  }
  ARROW_ASSIGN_OR_RAISE(auto owned, arrow::AllocateBuffer(length));
  if (length > 0) {
    std::memcpy(owned->mutable_data(), data, static_cast<size_t>(length));
  }
  return parseArrowBatchFrames(
      schema, std::shared_ptr<arrow::Buffer>(std::move(owned)), task_id);
}

}  // namespace detail

namespace {

// Deserializes a single Velox PrestoVectorSerde page (IOBuf) into a RowVector.
facebook::velox::RowVectorPtr deserializeVeloxPage(
    folly::IOBuf& page,
    facebook::velox::memory::MemoryPool* pool,
    const facebook::velox::RowTypePtr& output_type) {
  auto coalesced = page.cloneCoalesced();
  auto byte_range = facebook::velox::ByteRange{
      const_cast<uint8_t*>(coalesced->data()),
      static_cast<int32_t>(coalesced->length()),
      0};
  auto input = std::make_unique<facebook::velox::BufferInputStream>(
      std::vector<facebook::velox::ByteRange>{byte_range});
  facebook::velox::RowVectorPtr result;
  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions opts;
  facebook::velox::getVectorSerde()->deserialize(
      input.get(), pool, output_type, &result, &opts);
  return result;
}

// Exports a Velox RowVector to an Arrow RecordBatch via Arrow C Data Interface.
// L2-3: When preserve_dictionary=true, dictionary-encoded vectors are exported
// as-is (Arrow IPC natively supports dictionary encoding). This reduces transfer
// size by 50-80% for low-cardinality columns. Arrow Flight Java client handles
// dictionary batches correctly via standard IPC deserialization.
// When preserve_dictionary=false (legacy), all vectors are flattened to flat
// encoding before export to avoid potential client-side misinterpretation.
arrow::Result<std::shared_ptr<arrow::RecordBatch>> veloxToArrowBatch(
    const facebook::velox::RowVectorPtr& row_vector,
    facebook::velox::memory::MemoryPool* pool,
    bool preserve_dictionary = true) {
  ArrowArray out_array;
  ArrowSchema out_schema;
  ArrowOptions arrow_opts;
  arrow_opts.flattenDictionary = !preserve_dictionary;
  arrow_opts.flattenConstant = true;
  facebook::velox::exportToArrow(row_vector, out_array, pool, arrow_opts);
  facebook::velox::exportToArrow(row_vector, out_schema, arrow_opts);
  ARROW_ASSIGN_OR_RAISE(auto batch,
      arrow::ImportRecordBatch(&out_array, &out_schema));
  return batch;
}

// RecordBatchReader that drains pre-serialized Arrow IPC pages from Velox
// OutputBufferManager and streams them directly without deserialization +
// re-export. Each page contains one or more [4-byte length | RecordBatch bytes]
// frames written by ArrowBatchVectorSerde.
class DirectBatchStreamReader : public arrow::RecordBatchReader {
 public:
  DirectBatchStreamReader(
      const std::string& task_id,
      int destination,
      int64_t max_bytes,
      facebook::velox::RowTypePtr output_type,
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      int64_t timeout_seconds = 300)
      : task_id_(task_id),
        destination_(destination),
        max_bytes_(max_bytes),
        output_type_(std::move(output_type)),
        pool_holder_(std::move(pool)),
        pool_(pool_holder_.get()),
        timeout_seconds_(timeout_seconds),
        start_time_(std::chrono::steady_clock::now()) {}

  ~DirectBatchStreamReader() override {
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start_time_)
                          .count();
    PANGROVA_LOG(INFO) << "event=flight_result_doget_direct_complete"
                      << " task_id=" << task_id_
                      << " page_count=" << total_pages_
                      << " batch_count=" << total_batches_
                      << " total_bytes=" << total_bytes_
                      << " duration_ms=" << elapsed_ms;
    FlightResultServer::unregisterTaskSerdeMode(task_id_);
    FlightResultServer::unregisterTaskOutputType(task_id_);
  }

  std::shared_ptr<arrow::Schema> schema() const override {
    if (cached_schema_) {
      return cached_schema_;
    }
    if (output_type_) {
      ArrowSchema arrow_schema;
      facebook::velox::exportToArrow(
          facebook::velox::BaseVector::create(output_type_, 0, pool_),
          arrow_schema);
      auto result = arrow::ImportSchema(&arrow_schema);
      if (result.ok()) {
        cached_schema_ = *result;
        return cached_schema_;
      }
    }
    return arrow::schema({});
  }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
    // Drain queued batches from a previous multi-frame page BEFORE checking
    // finished_. When getData() returns [data, data, null], the null sets
    // finished_=true but earlier pages are queued — they must be drained.
    if (!pending_batches_.empty()) {
      *out = std::move(pending_batches_.front());
      pending_batches_.pop_front();
      return arrow::Status::OK();
    }

    if (finished_) {
      *out = nullptr;
      return arrow::Status::OK();
    }

    ARROW_RETURN_NOT_OK(fetchAndParsePages());

    if (pending_batches_.empty()) {
      *out = nullptr;
      return arrow::Status::OK();
    }

    *out = std::move(pending_batches_.front());
    pending_batches_.pop_front();
    if (!cached_schema_ && *out) {
      cached_schema_ = (*out)->schema();
    }
    return arrow::Status::OK();
  }

 private:
  arrow::Status fetchAndParsePages() {
    auto promise = std::make_shared<folly::Promise<
        std::vector<std::unique_ptr<folly::IOBuf>>>>();
    auto future = promise->getSemiFuture();

    const auto& buffer_mgr =
        facebook::velox::exec::OutputBufferManager::getInstanceRef();
    if (!buffer_mgr) {
      return arrow::Status::Invalid("OutputBufferManager unavailable");
    }

    struct DataCallback {
      std::shared_ptr<folly::Promise<std::vector<std::unique_ptr<folly::IOBuf>>>> p;
      void operator()(
          std::vector<std::unique_ptr<folly::IOBuf>> pages,
          int64_t, std::vector<int64_t>) const {
        p->setValue(std::move(pages));
      }
    };
    bool found = buffer_mgr->getData(
        task_id_, destination_,
        static_cast<uint64_t>(max_bytes_), sequence_,
        DataCallback{promise});

    if (!found) {
      finished_ = true;
      return arrow::Status::OK();
    }

    auto timed_future = (timeout_seconds_ > 0)
        ? std::move(future).within(std::chrono::seconds(timeout_seconds_))
        : std::move(future);
    std::vector<std::unique_ptr<folly::IOBuf>> pages;
    try {
      pages = std::move(timed_future).get();
    } catch (const folly::FutureTimeout&) {
      PANGROVA_LOG(WARNING) << "event=flight_result_doget_direct_timeout"
                          << " task_id=" << task_id_
                          << " sequence=" << sequence_
                          << " timeout_seconds=" << timeout_seconds_;
      finished_ = true;
      return arrow::Status::IOError(
          "Result fetch timed out after " + std::to_string(timeout_seconds_)
          + "s for task_id=" + task_id_);
    }

    if (pages.empty()) {
      if (sequence_ > 0) {
        buffer_mgr->acknowledge(task_id_, destination_, sequence_);
      }
      buffer_mgr->deleteResults(task_id_, destination_);
      finished_ = true;
      return arrow::Status::OK();
    }

    int64_t page_count = 0;
    for (auto& page : pages) {
      if (!page) {
        finished_ = true;
        break;
      }
      auto coalesced = page->cloneCoalesced();
      auto page_length = static_cast<int64_t>(coalesced->length());
      total_bytes_ += page_length;
      ++page_count;

      // PERF-T14: Wrap coalesced IOBuf into a shared arrow::Buffer.
      // The custom deleter captures the IOBuf shared_ptr to extend its lifetime
      // through all SliceBuffers derived in parseArrowBatchFrames.
      auto iobuf_shared = std::shared_ptr<folly::IOBuf>(coalesced.release());
      auto parent_buffer = std::shared_ptr<arrow::Buffer>(
          new arrow::Buffer(iobuf_shared->data(), page_length),
          [iobuf_shared](arrow::Buffer* buf) { delete buf; });

      ARROW_RETURN_NOT_OK(parsePageFrames(parent_buffer));
    }

    sequence_ += page_count;
    buffer_mgr->acknowledge(task_id_, destination_, sequence_);
    total_pages_ += page_count;
    return arrow::Status::OK();
  }

  arrow::Status parsePageFrames(std::shared_ptr<arrow::Buffer> parent_buffer) {
    ARROW_ASSIGN_OR_RAISE(auto batches,
        detail::parseArrowBatchFrames(schema(), parent_buffer, task_id_));
    for (auto& batch : batches) {
      pending_batches_.push_back(std::move(batch));
      ++total_batches_;
    }
    return arrow::Status::OK();
  }

  std::string task_id_;
  int destination_;
  int64_t max_bytes_;
  int64_t sequence_ = 0;
  facebook::velox::RowTypePtr output_type_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_holder_;
  facebook::velox::memory::MemoryPool* pool_;
  mutable std::shared_ptr<arrow::Schema> cached_schema_;
  int64_t timeout_seconds_;
  bool finished_ = false;
  int64_t total_pages_ = 0;
  int64_t total_batches_ = 0;
  int64_t total_bytes_ = 0;
  std::chrono::steady_clock::time_point start_time_;
  std::deque<std::shared_ptr<arrow::RecordBatch>> pending_batches_;
};

// RecordBatchReader that drains pre-serialized pages from Velox
// OutputBufferManager, deserializes them via PrestoVectorSerde,
// and yields structured Arrow RecordBatches.
class OutputBufferBatchReader : public arrow::RecordBatchReader {
 public:
  OutputBufferBatchReader(
      const std::string& task_id,
      int destination,
      int64_t max_bytes,
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      facebook::velox::RowTypePtr output_type,
      int64_t timeout_seconds = 300)
      : task_id_(task_id),
        destination_(destination),
        max_bytes_(max_bytes),
        pool_holder_(std::move(pool)),
        pool_(pool_holder_.get()),
        output_type_(std::move(output_type)),
        timeout_seconds_(timeout_seconds),
        start_time_(std::chrono::steady_clock::now()) {}

  ~OutputBufferBatchReader() override {
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start_time_)
                          .count();
    PANGROVA_LOG(INFO) << "event=flight_result_doget_complete"
                      << " task_id=" << task_id_
                      << " page_count=" << total_pages_
                      << " total_bytes=" << total_bytes_
                      << " duration_ms=" << elapsed_ms;
    // Clean up cached output type after DoGet completes.
    // VeloxTaskRunner::cleanup() no longer removes it — the reader owns
    // the lifecycle to avoid race conditions where cleanup runs before DoGet.
    FlightResultServer::unregisterTaskOutputType(task_id_);
  }

  std::shared_ptr<arrow::Schema> schema() const override {
    if (cached_schema_) {
      return cached_schema_;
    }
    // Derive schema from Velox output type before first batch arrives.
    // RecordBatchStream calls schema() at initialization — returning an empty
    // schema causes "Tried to write record batch with different schema" when
    // the first real batch arrives with actual columns.
    if (output_type_) {
      ArrowSchema arrow_schema;
      facebook::velox::exportToArrow(
          facebook::velox::BaseVector::create(output_type_, 0, pool_),
          arrow_schema);
      auto result = arrow::ImportSchema(&arrow_schema);
      if (result.ok()) {
        cached_schema_ = *result;
        return cached_schema_;
      }
    }
    return arrow::schema({});
  }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
    // Drain queued vectors from a previous multi-page getData() response
    // BEFORE checking finished_. When getData() returns [data, data, null],
    // the null sets finished_=true but earlier pages are queued here — they
    // must be fully drained before signaling end-of-stream.
    if (!pending_vectors_.empty()) {
      auto rv = std::move(pending_vectors_.front());
      pending_vectors_.pop_front();
      ARROW_ASSIGN_OR_RAISE(auto batch, veloxToArrowBatch(rv, pool_));
      if (!cached_schema_) {
        cached_schema_ = batch->schema();
      }
      *out = std::move(batch);
      return arrow::Status::OK();
    }

    if (finished_) {
      *out = nullptr;
      return arrow::Status::OK();
    }

    auto promise = std::make_shared<folly::Promise<
        std::vector<std::unique_ptr<folly::IOBuf>>>>();
    auto future = promise->getSemiFuture();

    const auto& buffer_mgr =
        facebook::velox::exec::OutputBufferManager::getInstanceRef();
    if (!buffer_mgr) {
      return arrow::Status::Invalid("OutputBufferManager unavailable");
    }

    struct DataCallback {
      std::shared_ptr<folly::Promise<std::vector<std::unique_ptr<folly::IOBuf>>>> p;
      void operator()(
          std::vector<std::unique_ptr<folly::IOBuf>> pages,
          int64_t, std::vector<int64_t>) const {
        p->setValue(std::move(pages));
      }
    };
    bool found = buffer_mgr->getData(
        task_id_, destination_,
        static_cast<uint64_t>(max_bytes_), sequence_,
        DataCallback{promise});

    if (!found) {
      finished_ = true;
      *out = nullptr;
      return arrow::Status::OK();
    }

    auto timed_future = (timeout_seconds_ > 0)
        ? std::move(future).within(std::chrono::seconds(timeout_seconds_))
        : std::move(future);
    std::vector<std::unique_ptr<folly::IOBuf>> pages;
    try {
      pages = std::move(timed_future).get();
    } catch (const folly::FutureTimeout&) {
      PANGROVA_LOG(WARNING) << "event=flight_result_doget_timeout"
                          << " task_id=" << task_id_
                          << " sequence=" << sequence_
                          << " timeout_seconds=" << timeout_seconds_;
      finished_ = true;
      *out = nullptr;
      return arrow::Status::IOError(
          "Result fetch timed out after " + std::to_string(timeout_seconds_)
          + "s for task_id=" + task_id_);
    }

    if (pages.empty()) {
      if (sequence_ > 0) {
        buffer_mgr->acknowledge(task_id_, destination_, sequence_);
      }
      // Signal that this destination has consumed all output.
      // Without this, the Velox Task stays in kRunning because
      // partitionedOutputConsumed_ is never set — the task waits
      // for all destination buffers to be deleted before transitioning
      // to kFinished via setAllOutputConsumed().
      buffer_mgr->deleteResults(task_id_, destination_);
      finished_ = true;
      *out = nullptr;
      return arrow::Status::OK();
    }

    // Deserialize pages and merge into a single Arrow RecordBatch.
    std::vector<facebook::velox::RowVectorPtr> row_vectors;
    int64_t total_bytes = 0;
    int64_t page_count = 0;

    for (auto& page : pages) {
      if (!page) {
        finished_ = true;
        break;
      }
      total_bytes += static_cast<int64_t>(page->computeChainDataLength());
      ++page_count;
      try {
        LOG(INFO) << "event=flight_deserialize_attempt task_id=" << task_id_
                  << " output_type=" << (output_type_ ? output_type_->toString() : "null")
                  << " page_bytes=" << page->computeChainDataLength();
        auto rv = deserializeVeloxPage(*page, pool_, output_type_);
        if (rv && rv->size() > 0) {
          LOG(INFO) << "event=flight_deserialize_success task_id=" << task_id_
                    << " rows=" << rv->size()
                    << " actual_type=" << rv->type()->toString();
          row_vectors.push_back(std::move(rv));
        }
      } catch (const facebook::velox::VeloxException& ve) {
        PANGROVA_LOG(ERROR) << "event=flight_result_deserialize_failed"
                          << " task_id=" << task_id_
                          << " output_type=" << (output_type_ ? output_type_->toString() : "null")
                          << " velox_error=" << ve.message()
                          << " context=" << ve.context();
        return arrow::Status::SerializationError(
            "Failed to deserialize Velox page: " + ve.message());
      } catch (const std::exception& e) {
        PANGROVA_LOG(ERROR) << "event=flight_result_deserialize_failed"
                          << " task_id=" << task_id_
                          << " output_type=" << (output_type_ ? output_type_->toString() : "null")
                          << " error=" << e.what();
        return arrow::Status::SerializationError(
            "Failed to deserialize Velox page: " + std::string(e.what()));
      }
    }

    sequence_ += page_count;
    buffer_mgr->acknowledge(task_id_, destination_, sequence_);
    total_pages_ += page_count;
    total_bytes_ += total_bytes;

    if (row_vectors.empty()) {
      if (page_count == 0) {
        finished_ = true;
      }
      *out = nullptr;
      return arrow::Status::OK();
    }

    // Export the first RowVector to Arrow RecordBatch.
    // When multiple pages arrive in a single getData() call, queue them
    // and return one per ReadNext invocation. This avoids the per-column
    // arrow::Concatenate copy that the previous implementation performed.
    if (row_vectors.size() > 1) {
      for (size_t i = 1; i < row_vectors.size(); ++i) {
        pending_vectors_.push_back(std::move(row_vectors[i]));
      }
    }

    auto& first_rv = row_vectors[0];
    ARROW_ASSIGN_OR_RAISE(auto batch, veloxToArrowBatch(first_rv, pool_));

    if (!cached_schema_) {
      cached_schema_ = batch->schema();
    }

    *out = std::move(batch);

    return arrow::Status::OK();
  }

 private:
  std::string task_id_;
  int destination_;
  int64_t max_bytes_;
  int64_t sequence_ = 0;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_holder_;
  facebook::velox::memory::MemoryPool* pool_;
  facebook::velox::RowTypePtr output_type_;
  mutable std::shared_ptr<arrow::Schema> cached_schema_;
  int64_t timeout_seconds_;
  bool finished_ = false;
  int64_t total_pages_ = 0;
  int64_t total_bytes_ = 0;
  std::chrono::steady_clock::time_point start_time_;
  // Queued RowVectors from multi-page getData() responses. Drained one
  // per ReadNext call before fetching new data from OutputBufferManager.
  std::deque<facebook::velox::RowVectorPtr> pending_vectors_;
};

}  // namespace

FlightResultServer::FlightResultServer(FlightResultServerConfig config)
    : config_(std::move(config)) {}

FlightResultServer::~FlightResultServer() {
  if (running_.load()) {
    stop();
  }
}

int FlightResultServer::start() {
  CHECK(!running_.load()) << "FlightResultServer already running";

  auto location_result =
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", config_.port);
  CHECK(location_result.ok())
      << "Failed to create Flight location: "
      << location_result.status().ToString();

  arrow::flight::FlightServerOptions options(*location_result);

  // Configure TLS if certificate paths are provided.
  if (!config_.tls_cert_chain_path.empty()) {
    std::ifstream cert_file(config_.tls_cert_chain_path);
    std::ifstream key_file(config_.tls_private_key_path);
    if (!cert_file.good() || !key_file.good()) {
      PANGROVA_LOG(ERROR) << "event=flight_result_server_tls_error"
                        << " cert=" << config_.tls_cert_chain_path
                        << " key=" << config_.tls_private_key_path
                        << " detail=certificate_or_key_file_not_readable";
    } else {
      std::string cert_pem((std::istreambuf_iterator<char>(cert_file)),
                            std::istreambuf_iterator<char>());
      std::string key_pem((std::istreambuf_iterator<char>(key_file)),
                           std::istreambuf_iterator<char>());
      arrow::flight::CertKeyPair cert_key{cert_pem, key_pem};
      options.tls_certificates.push_back(cert_key);

      if (!config_.tls_root_ca_path.empty()) {
        std::ifstream ca_file(config_.tls_root_ca_path);
        if (ca_file.good()) {
          std::string ca_pem((std::istreambuf_iterator<char>(ca_file)),
                              std::istreambuf_iterator<char>());
          options.root_certificates = ca_pem;
          options.verify_client = true;
          PANGROVA_LOG(INFO) << "event=flight_result_server_mtls_enabled"
                            << " port=" << config_.port;
        }
      }
      PANGROVA_LOG(INFO) << "event=flight_result_server_tls_enabled"
                        << " port=" << config_.port;
    }
  } else {
    PANGROVA_LOG(WARNING) << "event=flight_result_server_no_tls"
                        << " port=" << config_.port
                        << " detail=binding_without_tls_or_auth";
  }

  auto init_status = Init(options);
  CHECK(init_status.ok())
      << "Failed to init Flight result server: " << init_status.ToString();

  bound_port_ = port();
  running_.store(true);
  instance_.store(this, std::memory_order_release);

  // Serve in a background thread (Serve() blocks).
  server_thread_ = std::thread([this]() {
    auto status = Serve();
    if (!status.ok()) {
      PANGROVA_LOG(ERROR) << "event=flight_result_serve_error"
                        << " error=" << status.ToString();
    }
    running_.store(false);
  });

  PANGROVA_LOG(INFO) << "event=flight_result_server_started"
                    << " port=" << bound_port_;
  return bound_port_;
}

void FlightResultServer::stop() {
  if (!running_.exchange(false)) {
    return;
  }
  instance_.store(nullptr, std::memory_order_release);

  auto status = Shutdown();
  if (!status.ok()) {
    PANGROVA_LOG(ERROR) << "event=flight_result_shutdown_error"
                      << " error=" << status.ToString();
  }

  if (server_thread_.joinable()) {
    server_thread_.join();
  }

  PANGROVA_LOG(INFO) << "event=flight_result_server_stopped"
                    << " port=" << bound_port_;
}

arrow::Status FlightResultServer::DoGet(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::Ticket& request,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
  // Ticket is the taskId string.
  std::string task_id = request.ticket;
  if (task_id.empty()) {
    return arrow::Status::Invalid(
        "NOT_FOUND: empty taskId in result ticket");
  }

  PANGROVA_LOG(INFO) << "event=flight_result_doget_start"
                    << " task_id=" << task_id;

  // Verify the task exists in OutputBufferManager.
  const auto& buffer_mgr =
      facebook::velox::exec::OutputBufferManager::getInstanceRef();
  if (!buffer_mgr) {
    return arrow::Status::Invalid(
        "NOT_FOUND: OutputBufferManager unavailable");
  }

  // Wait for the task to register its OutputBuffer. There is a race between
  // the Java-side DAG dispatch reporting "result task ready" (which triggers
  // the Flight DoGet from the coordinator) and the native Velox Task actually
  // being created and registering its PartitionedOutput in OutputBufferManager.
  // Retry with exponential backoff for up to ~5 seconds before giving up.
  auto stats = buffer_mgr->stats(task_id);
  if (!stats.has_value()) {
    constexpr int kMaxRetries = 25;
    constexpr int kInitialSleepMs = 10;
    for (int attempt = 0; attempt < kMaxRetries && !stats.has_value(); ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(
          kInitialSleepMs * (1 + attempt / 5)));
      stats = buffer_mgr->stats(task_id);
    }
  }
  if (!stats.has_value()) {
    PANGROVA_LOG(WARNING) << "event=flight_result_doget_not_found"
                        << " task_id=" << task_id;
    return arrow::Status::Invalid(
        "NOT_FOUND: taskId not registered in OutputBufferManager: " +
        task_id);
  }

  // Look up the output type from the running Velox task.
  auto output_type = FlightResultServer::lookupTaskOutputType(task_id);
  if (!output_type) {
    return arrow::Status::Invalid(
        "NOT_FOUND: output type unavailable for taskId: " + task_id +
        ". Task may have already completed.");
  }

  // Create a reader that pulls from OutputBufferManager.
  // Route based on serde mode: Arrow IPC streams directly, legacy path
  // deserializes PrestoVectorSerde then re-exports via Arrow C Data Interface.
  auto* bridge = pangrova::jni::NativeEngineLifecycle::instance().veloxMemoryGovernor();
  if (!bridge || !bridge->veloxMemoryManager()) {
    return arrow::Status::Invalid(
        "INTERNAL: Velox memory manager unavailable for result deserialization");
  }

  auto serde_mode = lookupTaskSerdeMode(task_id);
  std::shared_ptr<arrow::RecordBatchReader> reader;

  if (serde_mode == "arrow_batch") {
    // Arrow IPC direct streaming: pages are pre-serialized RecordBatch frames.
    // No deserialization + re-export needed — stream directly to Flight client.
    auto pool = bridge->veloxMemoryManager()->addLeafPool(
        "flight_direct_" + task_id);
    reader = std::make_shared<DirectBatchStreamReader>(
        task_id,
        /*destination=*/0,
        config_.max_bytes_per_response,
        output_type,
        std::move(pool),
        config_.result_fetch_timeout_seconds);
    PANGROVA_LOG(INFO) << "event=flight_result_doget_mode"
                      << " task_id=" << task_id
                      << " mode=arrow_batch_direct";
  } else {
    // Legacy path: PrestoVectorSerde deserialize → Arrow C Data Interface export.
    auto deser_pool = bridge->veloxMemoryManager()->addLeafPool(
        "flight_result_" + task_id);
    reader = std::make_shared<OutputBufferBatchReader>(
        task_id,
        /*destination=*/0,
        config_.max_bytes_per_response,
        std::move(deser_pool),
        output_type,
        config_.result_fetch_timeout_seconds);
  }

  // Wrap in RecordBatchStream for Flight wire format.
  *stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);

  return arrow::Status::OK();
}

}  // namespace pangrova::result
