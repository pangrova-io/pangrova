// PangrovaExchangeSource: Velox ExchangeSource implementation for Pangrova's
// distributed shuffle protocol.
//
// C++ Flight client pulls data via Arrow Flight DoGet. Missing endpoints and
// transport failures fail the Velox task; there is no JVM data fallback.
//
// Registered as a factory with Velox's ExchangeSource::registerFactory()
// during runtime initialization.
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include <arrow/flight/client.h>
#include <folly/futures/SharedPromise.h>

#include "velox/exec/ExchangeQueue.h"
#include "velox/exec/ExchangeSource.h"
#include "engine/velox/exchange/PendingEndpointRegistry.h"

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

// Prevents endpoint metadata loss from becoming a silent TLS downgrade.
bool flightTransportSecurityCompatible(
    bool local_tls_enabled, std::string_view endpoint_tls_identity);

// Keeps the received Arrow buffer alive across serialized page and IOBuf clones.
std::unique_ptr<folly::IOBuf> retainFlightPageBuffer(
    std::shared_ptr<arrow::Buffer> owner, const uint8_t* payload, size_t size);

}  // namespace detail

// ExchangeSource that bridges Pangrova's exchange service with Velox.
//
// Reads an upstream worker's FlightShuffleServer directly from Native code.
class PangrovaExchangeSource
    : public facebook::velox::exec::ExchangeSource {
 public:
  PangrovaExchangeSource(
      const std::string& remoteTaskId,
      int destination,
      std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
      facebook::velox::memory::MemoryPool* pool);

  bool shouldRequestLocked() override;

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {};
  }

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) override;

  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override;

  void close() override;

  // Set the upstream Flight endpoint for active fetch mode.
  // Must be called before the first request() if Flight mode is desired.
  // Once set, request() will use Arrow Flight DoGet instead of waiting
  // for Java to push data via enqueueData().
  void setFlightEndpoint(PendingEndpoint endpoint);

  // Returns true if this source is configured for active Flight fetch.
  bool isFlightMode() const { return flight_configured_.load(); }

  // Factory registration — call during runtime init.
  static void registerExchangeSourceFactory();

  // Configures process-local mutual TLS credentials for every Native exchange
  // client. Configuration is replaced atomically during runtime init.
  static void configureTransportSecurity(
      bool enabled,
      const std::string& certificate_chain_path,
      const std::string& private_key_path,
      const std::string& root_ca_path,
      const std::string& local_tls_identity);

 private:
  // Active mode: fetch data from upstream Flight server
  folly::SemiFuture<Response> fetchViaFlight(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait,
      bool dataSizesOnly = false);
  folly::SemiFuture<Response> fetchViaExternal(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait,
      bool dataSizesOnly = false);
  void performExternalRead(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait,
      bool dataSizesOnly,
      std::shared_ptr<folly::SharedPromise<Response>> promise);

  // Runs on the lifecycle-owned blocking-I/O executor. Exactly one invocation
  // may be in flight for a source; all callers wait on flight_read_promise_.
  void performFlightRead(
      uint32_t maxBytes,
      bool dataSizesOnly,
      std::shared_ptr<folly::SharedPromise<Response>> promise,
      std::shared_ptr<arrow::StopSource> stopSource);
  Response enqueueFlightBatch(
      std::shared_ptr<arrow::RecordBatch> batch,
      uint32_t maxBytes);
  Response enqueueExternalBatch(std::string data, uint32_t maxBytes);
  folly::SemiFuture<Response> waitForFlightRead(
      std::shared_ptr<folly::SharedPromise<Response>> promise,
      std::chrono::microseconds maxWait);
  void finishFlightReadSuccess(
      const std::shared_ptr<folly::SharedPromise<Response>>& promise,
      Response response);
  void finishFlightReadFailure(
      const std::shared_ptr<folly::SharedPromise<Response>>& promise,
      std::string message);
  void clearRequestPending();

  std::mutex data_mutex_;
  std::atomic<bool> closed_{false};

  // Flight client state (active mode)
  // Transport liveness is enforced by gRPC keepalive, not a Flight call
  // deadline. A call deadline would cap the lifetime of a healthy long query.
  std::atomic<bool> flight_configured_{false};
  std::string flight_host_;
  int flight_port_ = 0;
  std::string flight_ticket_;
  std::string flight_source_task_id_;
  std::string flight_tls_identity_;
  int flight_partition_id_ = 0;
  std::shared_ptr<arrow::flight::FlightClient> flight_client_;
  // Persistent stream reader for the DoGet call. Shared ownership lets close()
  // safely call Cancel while the I/O executor is blocked in Next().
  std::shared_ptr<arrow::flight::FlightStreamReader> flight_reader_;
  std::shared_ptr<arrow::StopSource> flight_stop_source_;
  std::shared_ptr<folly::SharedPromise<Response>> flight_read_promise_;
  bool flight_read_in_flight_ = false;
  bool flight_stream_exhausted_ = false;
  std::string flight_terminal_error_;
  std::shared_ptr<arrow::RecordBatch> flight_prefetched_batch_;
  int64_t flight_prefetched_bytes_ = 0;
  std::shared_ptr<ExternalShuffleIngress> external_ingress_;
  std::optional<std::string> external_prefetched_batch_;
};

}  // namespace pangrova::velox
