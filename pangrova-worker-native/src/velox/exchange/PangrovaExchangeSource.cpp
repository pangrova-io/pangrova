// PangrovaExchangeSource: Implementation of Velox ExchangeSource for Pangrova's
// distributed shuffle protocol. Supports passive (Java push) and active
// (Arrow Flight pull) modes.
//
// Design: §4.10.2
#include "velox/exchange/PangrovaExchangeSource.h"

#include <sstream>
#include <thread>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <folly/io/IOBuf.h>
#include <glog/logging.h>

#include "observability/PangrovaLog.h"
#include "shuffle/ShuffleTicketKeys.h"
#include "shuffle/TicketSigner.h"
#include "velox/exchange/PendingEndpointRegistry.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/exec/SerializedPage.h"

namespace pangrova::velox {

// ---------------------------------------------------------------------------
// detail:: implementations
// ---------------------------------------------------------------------------
namespace detail {

StreamCompletion decideStreamCompletion(
    bool mid_stream_error,
    bool clean_eof,
    int64_t received,
    int64_t expected) {
  if (mid_stream_error) {
    return StreamCompletion::kFailedTruncated;
  }
  if (!clean_eof) {
    return StreamCompletion::kKeepReading;
  }
  // Clean EOF: check contract.
  if (expected < 0) {
    // No contract — trust the EOF.
    return StreamCompletion::kCompleteOk;
  }
  if (received >= expected) {
    return StreamCompletion::kCompleteOk;
  }
  // Short read under a contract — truncated.
  return StreamCompletion::kFailedTruncated;
}

bool flightShouldCancelOnIdle(
    bool next_in_flight,
    int64_t now_ms,
    int64_t next_started_ms,
    int64_t idle_budget_ms) {
  if (!next_in_flight) return false;
  if (next_started_ms < 0) return false;  // Not yet armed.
  if (idle_budget_ms <= 0) return false;   // Disabled.
  return (now_ms - next_started_ms) >= idle_budget_ms;
}

}  // namespace detail

using facebook::velox::exec::SerializedPage;
using ContinuePromise = facebook::velox::ContinuePromise;

// P1-EXCH-3: Maximum number of pending batches before backpressure kicks in.
static constexpr size_t kMaxPendingBatches = 64;

// Lock ordering (P1-EXCH-2):
//   data_mutex_ is acquired BEFORE any ExchangeQueue::mutex_ operation.
//   queue_->enqueueLocked() is called while data_mutex_ is held.
//   This ordering must be maintained to prevent deadlocks.

PangrovaExchangeSource::PangrovaExchangeSource(
    const std::string& remoteTaskId,
    int destination,
    std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
    facebook::velox::memory::MemoryPool* pool)
    : ExchangeSource(remoteTaskId, destination, std::move(queue), pool) {
  // Check PendingEndpointRegistry for a pre-registered Flight endpoint.
  // Java sends CONNECT_EXCHANGE_SOURCE before the Velox ExchangeNode creates
  // this source, so the endpoint is already in the registry by the time
  // the factory constructs us.
  auto pending = PendingEndpointRegistry::instance().take(remoteTaskId);
  if (pending.has_value()) {
    setFlightEndpoint(std::move(*pending));
  }
}

bool PangrovaExchangeSource::shouldRequestLocked() {
  // We should request if there's no pending request and we're not done.
  if (at_end_ || closed_ || request_pending_) {
    return false;
  }
  request_pending_ = true;
  return true;
}

folly::SemiFuture<PangrovaExchangeSource::Response>
PangrovaExchangeSource::request(
    uint32_t maxBytes,
    std::chrono::microseconds maxWait) {
  // Dispatch between active Flight mode and passive Java push mode.
  if (flight_configured_.load()) {
    return fetchViaFlight(maxBytes, maxWait);
  }
  return waitForPushedData(maxBytes, maxWait);
}

// Passive mode: drain data pushed by Java via enqueueData().
folly::SemiFuture<PangrovaExchangeSource::Response>
PangrovaExchangeSource::waitForPushedData(
    uint32_t maxBytes,
    std::chrono::microseconds maxWait) {
  std::vector<ContinuePromise> promises;
  int64_t totalBytes = 0;
  bool atEndAndDrained = false;
  std::vector<int64_t> remainingBytes;

  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    request_pending_ = false;

    // Drain available data up to maxBytes.
    while (!pending_entries_.empty()) {
      int64_t pageSize = pending_entries_.front().size_bytes;
      if (totalBytes > 0 && totalBytes + pageSize > maxBytes) {
        break;  // Would exceed budget.
      }
      auto& entry = pending_entries_.front();
      auto page = std::make_unique<SerializedPage>(
          std::move(entry.data), /*onDestructionCb=*/nullptr,
          std::optional<int64_t>(entry.num_rows));
      pending_entries_.pop_front();
      totalBytes += pageSize;

      // Enqueue into the ExchangeQueue for Velox to consume.
      queue_->enqueueLocked(std::move(page), promises);
    }

    if (at_end_ && pending_entries_.empty()) {
      // Signal end of data.
      queue_->enqueueLocked(nullptr, promises);
    }

    atEndAndDrained = at_end_ && pending_entries_.empty();

    // Collect remaining sizes — direct iteration on deque (no move needed).
    for (const auto& entry : pending_entries_) {
      remainingBytes.push_back(entry.size_bytes);
    }
  }

  // Fulfill any promises for waiting consumers.
  for (auto& promise : promises) {
    promise.setValue();
  }

  return folly::makeSemiFuture(Response{
      totalBytes,
      atEndAndDrained,
      std::move(remainingBytes)});
}

folly::SemiFuture<PangrovaExchangeSource::Response>
PangrovaExchangeSource::requestDataSizes(
    std::chrono::microseconds maxWait) {
  std::vector<int64_t> sizes;
  bool atEndAndDrained = false;
  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    // Direct iteration on deque — no move+re-push needed.
    for (const auto& entry : pending_entries_) {
      sizes.push_back(entry.size_bytes);
    }
    atEndAndDrained = at_end_ && pending_entries_.empty();
  }
  return folly::makeSemiFuture(
      Response{0, atEndAndDrained, std::move(sizes)});
}

void PangrovaExchangeSource::close() {
  std::lock_guard<std::mutex> lock(data_mutex_);
  closed_ = true;
  // Clear pending data.
  while (!pending_entries_.empty()) pending_entries_.pop_front();
  // Flight reader and client are NOT reset here — they are destroyed
  // when this ExchangeSource object is destroyed. This avoids a UAF
  // race with fetchViaFlight() which may be calling flight_reader_->Next()
  // outside the lock. The closed_ flag causes fetchViaFlight() to break
  // out of its read loop on the next iteration.
}

bool PangrovaExchangeSource::enqueueData(
    std::unique_ptr<folly::IOBuf> data,
    int64_t numRows) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  if (closed_ || at_end_) return false;
  // Backpressure — reject when queue exceeds capacity limit.
  if (pending_entries_.size() >= kMaxPendingBatches) {
    return false;
  }
  int64_t size = data ? static_cast<int64_t>(data->computeChainDataLength()) : 0;
  pending_entries_.push_back(PendingEntry{std::move(data), numRows, size});
  return true;
}

void PangrovaExchangeSource::noMoreData() {
  std::lock_guard<std::mutex> lock(data_mutex_);
  at_end_ = true;
}

void PangrovaExchangeSource::setFlightEndpoint(PendingEndpoint endpoint) {
  // Create Arrow Flight client connection outside the lock to avoid
  // holding data_mutex_ during network I/O.
  auto location_result = arrow::flight::Location::ForGrpcTcp(endpoint.host, endpoint.port);
  CHECK(location_result.ok())
      << "Failed to create Flight location: " << location_result.status().ToString();

  auto client_result = arrow::flight::FlightClient::Connect(*location_result);
  CHECK(client_result.ok())
      << "Failed to connect Flight client: " << client_result.status().ToString();

  // L2-9: Build the ticket and initiate DoGet immediately (prefetch).
  // This eliminates the 50-100ms latency of the first request() call by
  // opening the DoGet stream eagerly. The upstream worker's data is ready
  // by the time the endpoint is registered in PendingEndpointRegistry.
  std::ostringstream oss;
  oss << endpoint.executor_handle << "\n"
      << endpoint.query_id << "\n"
      << endpoint.stage_id << "\n"
      << endpoint.partition_id;
  std::string payload = oss.str();

  auto& keys = pangrova::shuffle::ShuffleTicketKeys::instance();
  std::string signed_ticket;
  if (keys.enabled()) {
    pangrova::shuffle::TicketSigner signer(keys.currentKey());
    signed_ticket = signer.sign(payload);
  } else {
    signed_ticket = std::move(payload);
  }
  arrow::flight::Ticket ticket{std::move(signed_ticket)};

  // Attempt DoGet with a single try (non-blocking best-effort prefetch).
  // If it fails here, fetchViaFlight() will retry with full backoff logic.
  arrow::flight::FlightCallOptions call_options;
  call_options.timeout =
      arrow::flight::TimeoutDuration{
          static_cast<double>(kFlightCallTimeoutMs) / 1000.0};
  auto reader_result = (*client_result)->DoGet(call_options, ticket);

  std::unique_ptr<arrow::flight::FlightStreamReader> prefetched_reader;
  if (reader_result.ok()) {
    prefetched_reader = std::move(*reader_result);
  } else {
    PANGROVA_LOG(WARNING)
        << "event=flight_prefetch_failed"
        << " error=" << reader_result.status().ToString()
        << " (will retry in fetchViaFlight)";
  }

  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    flight_host_ = endpoint.host;
    flight_port_ = endpoint.port;
    flight_executor_handle_ = endpoint.executor_handle;
    flight_query_id_ = endpoint.query_id;
    flight_stage_id_ = endpoint.stage_id;
    flight_partition_id_ = endpoint.partition_id;
    flight_client_ = std::move(*client_result);
    if (prefetched_reader) {
      flight_reader_ = std::move(prefetched_reader);
      flight_consecutive_failures_.store(0);
    }
  }
  flight_configured_.store(true);

  PANGROVA_LOG(INFO) << "event=flight_endpoint_set host=" << endpoint.host
                    << " port=" << endpoint.port
                    << " executor_handle=" << endpoint.executor_handle
                    << " query_id=" << endpoint.query_id
                    << " stage_id=" << endpoint.stage_id
                    << " partition_id=" << endpoint.partition_id
                    << " prefetched=" << (flight_reader_ != nullptr);
}

folly::SemiFuture<PangrovaExchangeSource::Response>
PangrovaExchangeSource::fetchViaFlight(
    uint32_t maxBytes,
    std::chrono::microseconds maxWait) {
  std::vector<ContinuePromise> promises;
  int64_t totalBytes = 0;
  bool atEnd = false;

  {
    std::unique_lock<std::mutex> lock(data_mutex_);
    request_pending_ = false;

    if (flight_stream_exhausted_) {
      // Already finished — signal end.
      queue_->enqueueLocked(nullptr, promises);
      for (auto& p : promises) { p.setValue(); }
      return folly::makeSemiFuture(Response{0, true, {}});
    }

    // Lazily open the DoGet stream on first fetch with retry logic.
    if (!flight_reader_ && flight_client_) {
      // Build multiplexed ticket: "handle\nquery_id\nstage_id\npartition_id"
      std::ostringstream oss;
      oss << flight_executor_handle_ << "\n"
          << flight_query_id_ << "\n"
          << flight_stage_id_ << "\n"
          << flight_partition_id_;
      std::string payload = oss.str();

      // F-NATIVE-SHUF-04: Sign the ticket with HMAC if a shuffle key is configured.
      // TicketSigner produces "payload|expiry|hmac" when enabled, or passes through
      // the plaintext payload when no key is set (unsecured/legacy deployments).
      auto& keys = pangrova::shuffle::ShuffleTicketKeys::instance();
      std::string signed_ticket;
      if (keys.enabled()) {
        pangrova::shuffle::TicketSigner signer(keys.currentKey());
        signed_ticket = signer.sign(payload);
      } else {
        signed_ticket = std::move(payload);
      }
      arrow::flight::Ticket ticket{std::move(signed_ticket)};

      for (int attempt = 1; attempt <= kMaxFlightRetries; ++attempt) {
        arrow::flight::FlightCallOptions call_options;
        call_options.timeout =
            arrow::flight::TimeoutDuration{
                static_cast<double>(kFlightCallTimeoutMs) / 1000.0};
        auto reader_result = flight_client_->DoGet(call_options, ticket);
        if (reader_result.ok()) {
          flight_reader_ = std::move(*reader_result);
          flight_consecutive_failures_.store(0);
          break;
        }
        int failures = flight_consecutive_failures_.fetch_add(1) + 1;
        if (attempt < kMaxFlightRetries) {
          int backoff_ms = kFlightRetryBaseMs * (1 << (attempt - 1));
          PANGROVA_LOG(WARNING)
              << "event=flight_doget_retry"
              << " attempt=" << attempt
              << " max_attempts=" << kMaxFlightRetries
              << " backoff_ms=" << backoff_ms
              << " consecutive_failures=" << failures
              << " error=" << reader_result.status().ToString();
          // Release lock during backoff to avoid blocking other threads.
          // Using unique_lock::unlock()/lock() for RAII safety — the lock
          // is always in a valid state when the scope exits, even if an
          // exception were thrown during sleep (which cannot happen, but
          // this pattern is safer for future maintenance).
          lock.unlock();
          std::this_thread::sleep_for(
              std::chrono::milliseconds(backoff_ms));
          lock.lock();
        } else {
          PANGROVA_LOG(ERROR)
              << "event=flight_doget_exhausted"
              << " attempts=" << kMaxFlightRetries
              << " consecutive_failures=" << failures
              << " error=" << reader_result.status().ToString();
          // Signal permanent failure instead of falling back to passive mode.
          // Passive mode has no data pusher when Flight was the configured
          // transport, so falling back would cause an indefinite hang.
          // Setting at_end_ and enqueueing nullptr tells Velox the exchange
          // source has no more data, surfacing the error to the task.
          at_end_ = true;
          flight_stream_exhausted_ = true;
          queue_->enqueueLocked(nullptr, promises);
        }
      }
    }

    if (!flight_reader_) {
      // If at_end_ was set by retry exhaustion, fulfill promises and return
      // immediately. Falling through to waitForPushedData would work but
      // would skip fulfilling promises collected during enqueueLocked above.
      if (at_end_) {
        for (auto& p : promises) { p.setValue(); }
        return folly::makeSemiFuture(Response{0, true, {}});
      }
      // Fallback to passive mode.
      // Release lock and delegate.
    }
  }

  // If we failed to get a reader, fall back to passive.
  if (!flight_reader_) {
    return waitForPushedData(maxBytes, maxWait);
  }

  // Read batches from the Flight stream up to maxBytes.
  // Safety: close() sets closed_=true under data_mutex_ but no longer
  // resets flight_reader_ — the reader is only destroyed in the
  // destructor or when the stream is exhausted. This eliminates the
  // UAF race between close() and Next().
  while (totalBytes < static_cast<int64_t>(maxBytes)) {
    {
      std::lock_guard<std::mutex> lock(data_mutex_);
      if (closed_) {
        break;
      }
    }
    auto chunk_result = flight_reader_->Next();
    if (!chunk_result.ok()) {
      PANGROVA_LOG(ERROR) << "event=flight_read_error error="
                        << chunk_result.status().ToString();
      break;
    }

    auto& chunk = *chunk_result;
    if (!chunk.data) {
      // End of stream.
      atEnd = true;
      std::lock_guard<std::mutex> lock(data_mutex_);
      flight_stream_exhausted_ = true;
      at_end_ = true;
      queue_->enqueueLocked(nullptr, promises);
      break;
    }

    // The Flight stream carries serialized PrestoVectorSerde bytes in a
    // binary column. Extract the raw bytes and wrap in IOBuf for Velox.
    // P0-PERF-1: Use IOBuf::wrapBuffer with a custom destructor that holds
    // the Arrow RecordBatch alive, eliminating the per-page memcpy.
    auto num_rows = chunk.data->num_rows();
    auto batch_holder = chunk.data;  // shared_ptr keeps Arrow buffers alive
    auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(
        batch_holder->column(0));

    // Batch enqueue: acquire lock once for all pages in this RecordBatch
    // instead of per-page lock acquisition.
    {
      std::lock_guard<std::mutex> lock(data_mutex_);
      for (int64_t row = 0; row < num_rows; ++row) {
        int32_t length = 0;
        const uint8_t* value = binary_array->GetValue(row, &length);

        // Zero-copy wrap: IOBuf references Arrow's buffer directly.
        // The shared_ptr capture in freeFunction ensures the Arrow RecordBatch
        // (and its underlying buffers) outlive the IOBuf.
        auto iobuf = folly::IOBuf::takeOwnership(
            const_cast<uint8_t*>(value), length,
            [](void* /*buf*/, void* userData) {
              delete static_cast<std::shared_ptr<arrow::RecordBatch>*>(userData);
            },
            new std::shared_ptr<arrow::RecordBatch>(batch_holder));
        int64_t pageSize = static_cast<int64_t>(length);

        auto page = std::make_unique<SerializedPage>(
            std::move(iobuf), /*onDestructionCb=*/nullptr,
            std::optional<int64_t>(std::nullopt));

        queue_->enqueueLocked(std::move(page), promises);
        totalBytes += pageSize;
      }
    }
  }

  // Fulfill promises.
  for (auto& promise : promises) {
    promise.setValue();
  }

  return folly::makeSemiFuture(Response{totalBytes, atEnd, {}});
}

void PangrovaExchangeSource::registerExchangeSourceFactory() {
  // Register a factory that creates PangrovaExchangeSource instances.
  // Velox's Exchange operator calls this factory when it needs to read
  // from a remote task.
  ExchangeSource::registerFactory(
      [](const std::string& remoteTaskId,
         int destination,
         std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
         facebook::velox::memory::MemoryPool* pool)
          -> std::shared_ptr<ExchangeSource> {
        // P1-EXCH-4: Only handle Pangrova exchange tasks via canonical URI prefix.
        if (remoteTaskId.find("pangrova://") == 0) {
          return std::make_shared<PangrovaExchangeSource>(
              remoteTaskId, destination, std::move(queue), pool);
        }
        return nullptr;  // Not ours — let other factories handle it.
      });
}

}  // namespace pangrova::velox
