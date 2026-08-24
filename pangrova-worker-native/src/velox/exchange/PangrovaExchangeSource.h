// PangrovaExchangeSource: Velox ExchangeSource implementation for Pangrova's
// distributed shuffle protocol.
//
// Active mode (production): C++ Flight client pulls data via Arrow Flight DoGet.
// Mode is activated by calling setFlightEndpoint() before request().
//
// Passive mode (Flight fallback): Java ExchangeClient pushes data via
// enqueueData(). Retained as a degradation path when Flight DoGet fails
// after exhausting retries (e.g., upstream server unreachable). The active
// mode fetchViaFlight() falls back to waitForPushedData() automatically.
// Once all workers reliably support native Flight, passive mode can be
// removed. Target removal: v0.3.0.
//
// Registered as a factory with Velox's ExchangeSource::registerFactory()
// during runtime initialization.
#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include <arrow/flight/client.h>

#include "velox/exec/ExchangeQueue.h"
#include "velox/exec/ExchangeSource.h"
#include "velox/exchange/PendingEndpointRegistry.h"

namespace pangrova::velox {

// ---------------------------------------------------------------------------
// detail:: — extracted decision functions for unit-testing.
// ---------------------------------------------------------------------------
namespace detail {

// Stream completion classification (F-CHAIN-SHUFFLE-03).
enum class StreamCompletion {
  kKeepReading,       // Neither error nor EOF: keep pulling.
  kCompleteOk,        // Clean completion.
  kFailedTruncated,   // Truncated / producer torn down before committing.
};

// Decides whether the shuffle stream is complete, truncated, or should keep
// reading, based on terminal signals and batch counts.
StreamCompletion decideStreamCompletion(
    bool mid_stream_error,
    bool clean_eof,
    int64_t received,
    int64_t expected);

// Decides whether a Flight DoGet read has been idle long enough to cancel
// (F-CHAIN-SHUFFLE-05). Returns true if cancellation is warranted.
bool flightShouldCancelOnIdle(
    bool next_in_flight,
    int64_t now_ms,
    int64_t next_started_ms,
    int64_t idle_budget_ms);

}  // namespace detail

// ExchangeSource that bridges Pangrova's exchange service with Velox.
//
// Passive mode (default): Remote task data is pushed via enqueueData()
// from the Pangrova exchange client (JNI side), and Velox pulls via request().
//
// Active mode (Flight): When setFlightEndpoint() is called, request()
// actively fetches data from the upstream worker's FlightShuffleServer
// via Arrow Flight DoGet, bypassing Java entirely.
class PangrovaExchangeSource
    : public facebook::velox::exec::ExchangeSource {
 public:
  PangrovaExchangeSource(
      const std::string& remoteTaskId,
      int destination,
      std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
      facebook::velox::memory::MemoryPool* pool);

  bool shouldRequestLocked() override;

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) override;

  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override;

  void close() override;

  // --- Passive mode (deprecated — use Flight active mode) ---

  // Called from JNI/exchange client to push serialized data pages.
  // Thread-safe: may be called from any thread.
  // Returns false if the queue is closed, at end, or full (backpressure).
  [[deprecated("Use active Flight mode via setFlightEndpoint() instead")]]
  bool enqueueData(std::unique_ptr<folly::IOBuf> data, int64_t numRows);

  // Signal that no more data will arrive from this source.
  [[deprecated("Use active Flight mode via setFlightEndpoint() instead")]]
  void noMoreData();

  // --- Active mode (Flight pull) ---

  // Set the upstream Flight endpoint for active fetch mode.
  // Must be called before the first request() if Flight mode is desired.
  // Once set, request() will use Arrow Flight DoGet instead of waiting
  // for Java to push data via enqueueData().
  void setFlightEndpoint(PendingEndpoint endpoint);

  // Returns true if this source is configured for active Flight fetch.
  bool isFlightMode() const { return flight_configured_.load(); }

  // Factory registration — call during runtime init.
  static void registerExchangeSourceFactory();

 private:
  // Passive mode: wait for Java to push data via enqueueData()
  folly::SemiFuture<Response> waitForPushedData(
      uint32_t maxBytes, std::chrono::microseconds maxWait);

  // Active mode: fetch data from upstream Flight server
  folly::SemiFuture<Response> fetchViaFlight(
      uint32_t maxBytes, std::chrono::microseconds maxWait);

  // Consolidated pending entry replacing three independent queues.
  // Eliminates sync risk from independent queue operations.
  struct PendingEntry {
    std::unique_ptr<folly::IOBuf> data;
    int64_t num_rows;
    int64_t size_bytes;
  };

  std::mutex data_mutex_;
  std::deque<PendingEntry> pending_entries_;
  bool at_end_ = false;
  bool closed_ = false;
  bool request_pending_ = false;

  // Flight client state (active mode)
  static constexpr int kMaxFlightRetries = 3;
  static constexpr int kFlightRetryBaseMs = 100;
  // Timeout for individual Flight DoGet and Next() calls. Prevents
  // indefinite blocking when the upstream server is unreachable.
  static constexpr int kFlightCallTimeoutMs = 30000;
  std::atomic<bool> flight_configured_{false};
  std::atomic<int> flight_consecutive_failures_{0};
  std::string flight_host_;
  int flight_port_ = 0;
  int64_t flight_executor_handle_ = 0;
  std::string flight_query_id_;
  int flight_stage_id_ = 0;
  int flight_partition_id_ = 0;
  std::unique_ptr<arrow::flight::FlightClient> flight_client_;
  // Persistent stream reader for the DoGet call (reused across request() calls).
  std::unique_ptr<arrow::flight::FlightStreamReader> flight_reader_;
  bool flight_stream_exhausted_ = false;
};

}  // namespace pangrova::velox
