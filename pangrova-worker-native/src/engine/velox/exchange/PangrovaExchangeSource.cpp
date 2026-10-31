// PangrovaExchangeSource: Implementation of Velox ExchangeSource for Pangrova's
// distributed shuffle protocol. Supports passive (Java push) and active
// (Arrow Flight pull) modes.
//
// Design: §4.10.2
#include "engine/velox/exchange/PangrovaExchangeSource.h"

#include <algorithm>
#include <fstream>
#include <limits>
#include <stdexcept>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <folly/Executor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>
#include <glog/logging.h>

#include "bridge/RuntimeInitializer.h"
#include "engine/observability/PangrovaLog.h"
#include "engine/velox/exchange/PendingEndpointRegistry.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/exec/SerializedPage.h"

namespace pangrova::velox {

namespace {

struct FlightClientSecurity {
  bool enabled = false;
  std::string certificate_chain;
  std::string private_key;
  std::string root_ca;
};

std::mutex g_flight_client_security_mutex;
std::shared_ptr<const FlightClientSecurity> g_flight_client_security =
    std::make_shared<const FlightClientSecurity>();

using ExchangeResponse = facebook::velox::exec::ExchangeSource::Response;

struct FlightReadWaitState {
  bool claim() {
    std::lock_guard<std::mutex> lock(mutex);
    if (completed) {
      return false;
    }
    completed = true;
    return true;
  }

  std::mutex mutex;
  bool completed = false;
  folly::Promise<ExchangeResponse> result;
};

std::string readRequiredPem(
    const std::string& path, const std::string& description) {
  std::ifstream input(path);
  if (!input.good()) {
    throw std::runtime_error(
        "Failed to read Native transport " + description);
  }
  std::string pem(
      (std::istreambuf_iterator<char>(input)),
      std::istreambuf_iterator<char>());
  if (pem.empty()) {
    throw std::runtime_error(
        "Native transport " + description + " is empty");
  }
  return pem;
}

std::shared_ptr<const FlightClientSecurity> flightClientSecuritySnapshot() {
  std::lock_guard<std::mutex> lock(g_flight_client_security_mutex);
  return g_flight_client_security;
}

}  // namespace

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

bool flightTransportSecurityCompatible(
    bool local_tls_enabled, std::string_view endpoint_tls_identity) {
  return local_tls_enabled == !endpoint_tls_identity.empty();
}

}  // namespace detail

using facebook::velox::exec::SerializedPage;
using ContinuePromise = facebook::velox::ContinuePromise;

// Concurrency rule (P1-EXCH-2): never hold data_mutex_ and
// ExchangeQueue::mutex_ at the same time. Network state is snapshotted first;
// queue mutation and promise fulfillment happen in separate critical sections.

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
  auto pending = PendingEndpointRegistry::instance().takeEndpoint(remoteTaskId);
  if (!pending.has_value()) {
    throw std::runtime_error(
        "No task-scoped endpoint for exchange source " + remoteTaskId);
  }
  if (pending->external_ingress) {
    external_ingress_ = std::move(pending->external_ingress);
  } else {
    setFlightEndpoint(std::move(*pending));
  }
}

bool PangrovaExchangeSource::shouldRequestLocked() {
  if (closed_.load()) {
    return false;
  }
  // A timed-out long poll can return an empty response just as the in-flight
  // read reaches EOF. ExchangeClient may put that source back in its empty
  // queue after atEnd_ becomes true. Let it request once more so the terminal
  // response removes the source; rejecting it violates ExchangeClient's
  // empty-source invariant and strands the downstream task.
  return !requestPending_.exchange(true);
}

folly::SemiFuture<PangrovaExchangeSource::Response>
PangrovaExchangeSource::request(
    uint32_t maxBytes,
    std::chrono::microseconds maxWait) {
  if (!flight_configured_.load()) {
    if (external_ingress_) {
      return fetchViaExternal(maxBytes, maxWait);
    }
    return folly::makeSemiFuture<Response>(
        folly::make_exception_wrapper<std::runtime_error>(
            "Native exchange source has no Flight endpoint"));
  }
  return fetchViaFlight(maxBytes, maxWait);
}

folly::SemiFuture<PangrovaExchangeSource::Response>
PangrovaExchangeSource::requestDataSizes(
    std::chrono::microseconds maxWait) {
  // Neither transport exposes the next serialized page size without taking
  // it. Probe exactly one batch. Small batches may be delivered immediately as
  // permitted by Velox; larger batches remain source-owned until request()
  // admits their reported size against the ExchangeClient queue budget.
  constexpr uint32_t kProbeBytes = 1024 * 1024;
  if (external_ingress_) {
    return fetchViaExternal(kProbeBytes, maxWait, true);
  }
  if (flight_configured_.load()) {
    return fetchViaFlight(kProbeBytes, maxWait, true);
  }
  clearRequestPending();
  return folly::makeSemiFuture<Response>(
      folly::make_exception_wrapper<std::runtime_error>(
          "Native exchange source has no configured transport"));
}

void PangrovaExchangeSource::close() {
  closed_.store(true);

  std::shared_ptr<arrow::StopSource> stopSource;
  std::shared_ptr<arrow::flight::FlightStreamReader> reader;
  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    stopSource = flight_stop_source_;
    reader = flight_reader_;
    flight_reader_.reset();
    flight_client_.reset();
    flight_prefetched_batch_.reset();
    flight_prefetched_bytes_ = 0;
    external_prefetched_batch_.reset();
  }
  // Both operations are thread-safe cancellation signals. They are issued
  // outside data_mutex_ because the gRPC transport may synchronously wake the
  // I/O thread, which then needs the same mutex to publish completion.
  if (stopSource) {
    stopSource->RequestStop(
        arrow::Status::Cancelled("Native exchange source closed"));
  }
  if (external_ingress_) {
    external_ingress_->cancel();
  }
  if (reader) {
    reader->Cancel();
  }
  clearRequestPending();
}

void PangrovaExchangeSource::setFlightEndpoint(PendingEndpoint endpoint) {
  // Endpoint registration is control-plane metadata handling only. Network
  // connection and DoGet are deferred to the dedicated Native I/O executor so
  // task admission and Velox driver threads can never block on a remote peer.
  auto security = flightClientSecuritySnapshot();
  if (!detail::flightTransportSecurityCompatible(
          security->enabled, endpoint.tls_identity)) {
    throw std::runtime_error(
        security->enabled
            ? "Refusing plaintext Native exchange endpoint while transport TLS is enabled"
            : "Refusing TLS Native exchange endpoint without local transport credentials");
  }

  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    if (flight_configured_.load()) {
      throw std::logic_error("Native exchange endpoint is immutable");
    }
    flight_host_ = endpoint.host;
    flight_port_ = endpoint.port;
    flight_ticket_ = std::move(endpoint.signed_ticket);
    flight_source_task_id_ = std::move(endpoint.source_task_id);
    flight_tls_identity_ = std::move(endpoint.tls_identity);
    flight_partition_id_ = endpoint.output_partition_id;
  }
  flight_configured_.store(true);

}

void PangrovaExchangeSource::configureTransportSecurity(
    bool enabled,
    const std::string& certificate_chain_path,
    const std::string& private_key_path,
    const std::string& root_ca_path,
    const std::string& local_tls_identity) {
  const bool has_any_value = !certificate_chain_path.empty() ||
      !private_key_path.empty() || !root_ca_path.empty() ||
      !local_tls_identity.empty();
  if (!enabled && has_any_value) {
    throw std::invalid_argument(
        "Transport TLS settings require enabled=true");
  }

  auto replacement = std::make_shared<FlightClientSecurity>();
  replacement->enabled = enabled;
  if (enabled) {
    if (certificate_chain_path.empty() || private_key_path.empty() ||
        root_ca_path.empty() || local_tls_identity.empty()) {
      throw std::invalid_argument(
          "Enabled transport TLS requires certificate, private key, root CA and identity");
    }
    replacement->certificate_chain = readRequiredPem(
        certificate_chain_path, "certificate chain");
    replacement->private_key = readRequiredPem(
        private_key_path, "private key");
    replacement->root_ca = readRequiredPem(root_ca_path, "root CA");
  }
  {
    std::lock_guard<std::mutex> lock(g_flight_client_security_mutex);
    g_flight_client_security = std::move(replacement);
  }
}

folly::SemiFuture<PangrovaExchangeSource::Response>
PangrovaExchangeSource::fetchViaFlight(
    uint32_t maxBytes,
    std::chrono::microseconds maxWait,
    bool dataSizesOnly) {
  std::shared_ptr<folly::SharedPromise<Response>> promise;
  std::shared_ptr<arrow::StopSource> stopSource;
  std::shared_ptr<arrow::RecordBatch> prefetchedBatch;
  int64_t prefetchedBytes = 0;
  bool scheduleRead = false;
  std::string terminalError;
  bool exhausted = false;
  bool closed = false;

  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    closed = closed_.load();
    terminalError = flight_terminal_error_;
    exhausted = flight_stream_exhausted_;
    if (!closed && terminalError.empty() && flight_prefetched_batch_) {
      prefetchedBytes = flight_prefetched_bytes_;
      if (!dataSizesOnly) {
        prefetchedBatch = std::move(flight_prefetched_batch_);
        flight_prefetched_bytes_ = 0;
      }
    } else if (!closed && terminalError.empty() && !exhausted) {
      if (!flight_read_in_flight_) {
        flight_read_in_flight_ = true;
        flight_read_promise_ =
            std::make_shared<folly::SharedPromise<Response>>();
        flight_stop_source_ = std::make_shared<arrow::StopSource>();
        scheduleRead = true;
      }
      promise = flight_read_promise_;
      stopSource = flight_stop_source_;
    }
  }

  if (closed) {
    clearRequestPending();
    return folly::makeSemiFuture(Response{0, false, {}});
  }
  if (!terminalError.empty()) {
    clearRequestPending();
    return folly::makeSemiFuture<Response>(
        folly::make_exception_wrapper<std::runtime_error>(terminalError));
  }
  if (prefetchedBytes > 0 && dataSizesOnly) {
    clearRequestPending();
    return folly::makeSemiFuture(
        Response{0, false, {prefetchedBytes}});
  }
  if (prefetchedBatch) {
    try {
      auto response = enqueueFlightBatch(std::move(prefetchedBatch), maxBytes);
      clearRequestPending();
      return folly::makeSemiFuture(std::move(response));
    } catch (const std::exception& error) {
      clearRequestPending();
      return folly::makeSemiFuture<Response>(
          folly::make_exception_wrapper<std::runtime_error>(error.what()));
    }
  }
  if (exhausted) {
    clearRequestPending();
    return folly::makeSemiFuture(Response{0, true, {}});
  }

  if (scheduleRead) {
    auto ioExecutor = pangrova::jni::globalBlockingIoExecutor();
    if (!ioExecutor) {
      finishFlightReadFailure(
          promise, "Native exchange I/O executor is unavailable");
    } else {
      auto self = std::static_pointer_cast<PangrovaExchangeSource>(
          shared_from_this());
      try {
        ioExecutor->add(
            [self, maxBytes, dataSizesOnly, promise, stopSource]() mutable {
              self->performFlightRead(
                  maxBytes,
                  dataSizesOnly,
                  std::move(promise),
                  std::move(stopSource));
            });
      } catch (const std::exception& e) {
        finishFlightReadFailure(
            promise,
            std::string("Failed to schedule Native exchange read: ") +
                e.what());
      }
    }
  }

  return waitForFlightRead(std::move(promise), maxWait);
}

folly::SemiFuture<PangrovaExchangeSource::Response>
PangrovaExchangeSource::fetchViaExternal(
    uint32_t maxBytes,
    std::chrono::microseconds maxWait,
    bool dataSizesOnly) {
  std::shared_ptr<folly::SharedPromise<Response>> promise;
  std::optional<std::string> prefetchedBatch;
  int64_t prefetchedBytes = 0;
  bool scheduleRead = false;
  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    if (external_prefetched_batch_.has_value()) {
      prefetchedBytes = static_cast<int64_t>(
          external_prefetched_batch_->size());
      if (!dataSizesOnly) {
        prefetchedBatch = std::move(external_prefetched_batch_);
        external_prefetched_batch_.reset();
      }
    } else if (!flight_read_in_flight_) {
      flight_read_in_flight_ = true;
      flight_read_promise_ = std::make_shared<folly::SharedPromise<Response>>();
      scheduleRead = true;
    }
    promise = flight_read_promise_;
  }
  if (prefetchedBytes > 0 && dataSizesOnly) {
    clearRequestPending();
    return folly::makeSemiFuture(
        Response{0, false, {prefetchedBytes}});
  }
  if (prefetchedBatch.has_value()) {
    try {
      auto response =
          enqueueExternalBatch(std::move(*prefetchedBatch), maxBytes);
      clearRequestPending();
      return folly::makeSemiFuture(std::move(response));
    } catch (const std::exception& error) {
      clearRequestPending();
      return folly::makeSemiFuture<Response>(
          folly::make_exception_wrapper<std::runtime_error>(error.what()));
    }
  }
  if (scheduleRead) {
    auto ioExecutor = pangrova::jni::globalBlockingIoExecutor();
    if (!ioExecutor) {
      finishFlightReadFailure(
          promise, "Native exchange I/O executor is unavailable");
    } else {
      auto self = std::static_pointer_cast<PangrovaExchangeSource>(
          shared_from_this());
      try {
        ioExecutor->add([self, maxBytes, maxWait, dataSizesOnly, promise]() {
          self->performExternalRead(
              maxBytes, maxWait, dataSizesOnly, promise);
        });
      } catch (const std::exception& error) {
        finishFlightReadFailure(promise, error.what());
      }
    }
  }
  return waitForFlightRead(std::move(promise), maxWait);
}

void PangrovaExchangeSource::performExternalRead(
    uint32_t maxBytes,
    std::chrono::microseconds maxWait,
    bool dataSizesOnly,
    std::shared_ptr<folly::SharedPromise<Response>> promise) {
  try {
    auto result = external_ingress_->take(maxWait);
    if (result.kind == ExternalShuffleIngress::ResultKind::kPending) {
      finishFlightReadSuccess(promise, Response{0, false, {}});
      return;
    }
    if (result.kind == ExternalShuffleIngress::ResultKind::kCancelled) {
      finishFlightReadSuccess(promise, Response{0, false, {}});
      return;
    }
    if (result.kind == ExternalShuffleIngress::ResultKind::kFailed) {
      finishFlightReadFailure(promise, std::move(result.error));
      return;
    }
    std::vector<ContinuePromise> queuePromises;
    if (result.kind == ExternalShuffleIngress::ResultKind::kFinished) {
      {
        std::lock_guard<std::mutex> queueLock(queue_->mutex());
        if (!atEnd_) {
          atEnd_ = true;
          queue_->enqueueLocked(nullptr, queuePromises);
        }
      }
      for (auto& queuePromise : queuePromises) {
        queuePromise.setValue();
      }
      finishFlightReadSuccess(promise, Response{0, true, {}});
      return;
    }
    const auto bytes = static_cast<int64_t>(result.data.size());
    if (dataSizesOnly && bytes > static_cast<int64_t>(maxBytes)) {
      bool closed = false;
      {
        std::lock_guard<std::mutex> lock(data_mutex_);
        closed = closed_.load();
        if (!closed && external_prefetched_batch_.has_value()) {
          throw std::logic_error(
              "External exchange already owns a prefetched batch");
        }
        if (!closed) {
          external_prefetched_batch_ = std::move(result.data);
        }
      }
      if (closed) {
        finishFlightReadSuccess(promise, Response{0, false, {}});
      } else {
        finishFlightReadSuccess(promise, Response{0, false, {bytes}});
      }
      return;
    }
    finishFlightReadSuccess(
        promise, enqueueExternalBatch(std::move(result.data), maxBytes));
  } catch (const std::exception& error) {
    finishFlightReadFailure(promise, error.what());
  }
}

folly::SemiFuture<PangrovaExchangeSource::Response>
PangrovaExchangeSource::waitForFlightRead(
    std::shared_ptr<folly::SharedPromise<Response>> promise,
    std::chrono::microseconds maxWait) {
  auto self = std::static_pointer_cast<PangrovaExchangeSource>(
      shared_from_this());
  if (maxWait.count() <= 0) {
    return promise->getSemiFuture().deferEnsure(
        [self]() { self->clearRequestPending(); });
  }

  auto ioExecutor = pangrova::jni::globalIoExecutor();
  auto* ioPool = dynamic_cast<folly::IOThreadPoolExecutor*>(ioExecutor.get());
  if (!ioPool) {
    return folly::makeSemiFuture<Response>(
               folly::make_exception_wrapper<std::runtime_error>(
                   "Native exchange timeout scheduler is unavailable"))
        .deferEnsure([self]() { self->clearRequestPending(); });
  }

  auto state = std::make_shared<FlightReadWaitState>();
  auto result = state->result.getSemiFuture();
  std::move(promise->getSemiFuture())
      .via(ioExecutor.get())
      .thenTry([state](folly::Try<Response>&& response) {
        if (state->claim()) {
          state->result.setTry(std::move(response));
        }
      });

  const auto roundedMillis = std::chrono::ceil<std::chrono::milliseconds>(
      maxWait);
  const auto timeoutMillis = static_cast<uint32_t>(std::min<int64_t>(
      std::max<int64_t>(1, roundedMillis.count()),
      std::numeric_limits<uint32_t>::max()));
  auto* eventBase = ioPool->getEventBase();
  try {
    eventBase->runInEventBaseThread(
        [eventBase, state, timeoutMillis]() {
          eventBase->runAfterDelay(
              [state]() {
                if (state->claim()) {
                  // End only this Velox long-poll. The blocking Flight Next()
                  // remains in flight and a later request receives its result.
                  state->result.setValue(Response{0, false, {}});
                }
              },
              timeoutMillis);
        });
  } catch (const std::exception& error) {
    if (state->claim()) {
      state->result.setException(
          folly::make_exception_wrapper<std::runtime_error>(error.what()));
    }
  }
  return std::move(result).deferEnsure(
      [self]() { self->clearRequestPending(); });
}

void PangrovaExchangeSource::performFlightRead(
    uint32_t maxBytes,
    bool dataSizesOnly,
    std::shared_ptr<folly::SharedPromise<Response>> promise,
    std::shared_ptr<arrow::StopSource> stopSource) {
  try {
    std::shared_ptr<arrow::flight::FlightClient> client;
    std::shared_ptr<arrow::flight::FlightStreamReader> reader;
    std::string host;
    std::string tlsIdentity;
    int port = 0;
    std::string ticketValue;
    {
      std::lock_guard<std::mutex> lock(data_mutex_);
      client = flight_client_;
      reader = flight_reader_;
      host = flight_host_;
      tlsIdentity = flight_tls_identity_;
      port = flight_port_;
      ticketValue = flight_ticket_;
    }
    if (closed_.load()) {
      finishFlightReadSuccess(promise, Response{0, false, {}});
      return;
    }

    if (!client) {
      auto security = flightClientSecuritySnapshot();
      auto locationResult = security->enabled
          ? arrow::flight::Location::ForGrpcTls(host, port)
          : arrow::flight::Location::ForGrpcTcp(host, port);
      if (!locationResult.ok()) {
        throw std::runtime_error(
            "Failed to create Flight location: " +
            locationResult.status().ToString());
      }
      auto clientOptions = arrow::flight::FlightClientOptions::Defaults();
      clientOptions.generic_options.emplace_back(
          "grpc.keepalive_time_ms", kFlightKeepaliveTimeMs);
      clientOptions.generic_options.emplace_back(
          "grpc.keepalive_timeout_ms", kFlightKeepaliveTimeoutMs);
      clientOptions.generic_options.emplace_back(
          "grpc.keepalive_permit_without_calls", 1);
      if (security->enabled) {
        clientOptions.tls_root_certs = security->root_ca;
        clientOptions.override_hostname = tlsIdentity;
        clientOptions.cert_chain = security->certificate_chain;
        clientOptions.private_key = security->private_key;
      }
      auto clientResult = arrow::flight::FlightClient::Connect(
          *locationResult, clientOptions);
      if (!clientResult.ok()) {
        throw std::runtime_error(
            "Failed to connect Flight client: " +
            clientResult.status().ToString());
      }
      client = std::shared_ptr<arrow::flight::FlightClient>(
          std::move(*clientResult));
      {
        std::lock_guard<std::mutex> lock(data_mutex_);
        flight_client_ = client;
      }
    }

    if (!reader) {
      arrow::flight::FlightCallOptions callOptions;
      callOptions.stop_token = stopSource->token();
      auto readerResult = client->DoGet(
          callOptions, arrow::flight::Ticket{ticketValue});
      if (!readerResult.ok()) {
        throw std::runtime_error(
            "Flight DoGet failed: " + readerResult.status().ToString());
      }
      reader = std::shared_ptr<arrow::flight::FlightStreamReader>(
          std::move(*readerResult));
      {
        std::lock_guard<std::mutex> lock(data_mutex_);
        flight_reader_ = reader;
      }
      if (closed_.load()) {
        reader->Cancel();
      }
    }

    auto chunkResult = reader->Next();
    if (!chunkResult.ok()) {
      if (closed_.load()) {
        finishFlightReadSuccess(promise, Response{0, false, {}});
        return;
      }
      throw std::runtime_error(
          "Flight exchange stream failed: " +
          chunkResult.status().ToString());
    }

    auto& chunk = *chunkResult;
    std::vector<ContinuePromise> queuePromises;
    if (!chunk.data) {
      {
        std::lock_guard<std::mutex> lock(data_mutex_);
        flight_stream_exhausted_ = true;
        flight_reader_.reset();
        flight_client_.reset();
      }
      reader.reset();
      client.reset();
      {
        std::lock_guard<std::mutex> queueLock(queue_->mutex());
        if (!atEnd_) {
          atEnd_ = true;
          queue_->enqueueLocked(nullptr, queuePromises);
        }
      }
      for (auto& queuePromise : queuePromises) {
        queuePromise.setValue();
      }
      finishFlightReadSuccess(promise, Response{0, true, {}});
      return;
    }

    if (chunk.data->num_columns() != 1 ||
        chunk.data->column(0)->type_id() != arrow::Type::BINARY) {
      throw std::runtime_error(
          "Flight shuffle batch must contain exactly one binary column");
    }

    int64_t totalBytes = 0;
    auto batchHolder = chunk.data;
    auto binaryArray = std::static_pointer_cast<arrow::BinaryArray>(
        batchHolder->column(0));
    for (int64_t row = 0; row < batchHolder->num_rows(); ++row) {
      if (binaryArray->IsNull(row)) {
        throw std::runtime_error("Flight shuffle batch contains a null page");
      }
      int32_t length = 0;
      binaryArray->GetValue(row, &length);
      totalBytes += static_cast<int64_t>(length);
    }

    if (dataSizesOnly && totalBytes > static_cast<int64_t>(maxBytes)) {
      bool closed = false;
      {
        std::lock_guard<std::mutex> lock(data_mutex_);
        closed = closed_.load();
        if (!closed && flight_prefetched_batch_) {
          throw std::logic_error(
              "Flight exchange already owns a prefetched batch");
        }
        if (!closed) {
          flight_prefetched_batch_ = std::move(batchHolder);
          flight_prefetched_bytes_ = totalBytes;
        }
      }
      if (closed) {
        finishFlightReadSuccess(promise, Response{0, false, {}});
      } else {
        finishFlightReadSuccess(
            promise, Response{0, false, {totalBytes}});
      }
      return;
    }
    finishFlightReadSuccess(
        promise, enqueueFlightBatch(std::move(batchHolder), maxBytes));
  } catch (const std::exception& e) {
    PANGROVA_LOG(ERROR)
        << "event=flight_exchange_read_failed error=" << e.what();
    finishFlightReadFailure(promise, e.what());
  }
}

PangrovaExchangeSource::Response PangrovaExchangeSource::enqueueFlightBatch(
    std::shared_ptr<arrow::RecordBatch> batch,
    uint32_t maxBytes) {
  if (!batch || batch->num_columns() != 1 ||
      batch->column(0)->type_id() != arrow::Type::BINARY) {
    throw std::invalid_argument("Invalid prefetched Flight shuffle batch");
  }
  auto binaryArray = std::static_pointer_cast<arrow::BinaryArray>(
      batch->column(0));
  std::vector<std::unique_ptr<SerializedPage>> pages;
  pages.reserve(static_cast<size_t>(batch->num_rows()));
  int64_t totalBytes = 0;
  for (int64_t row = 0; row < batch->num_rows(); ++row) {
    if (binaryArray->IsNull(row)) {
      throw std::runtime_error("Flight shuffle batch contains a null page");
    }
    int32_t length = 0;
    const uint8_t* value = binaryArray->GetValue(row, &length);
    totalBytes += static_cast<int64_t>(length);
    // The exchange queue can outlive the Arrow batch. Keep each page in an
    // owning buffer; Folly's takeOwnership ABI is not portable across the
    // macOS libc++ versions used by the build and dependency archives.
    auto data = folly::IOBuf::copyBuffer(value, static_cast<size_t>(length));
    pages.push_back(std::make_unique<SerializedPage>(
        std::move(data), nullptr, std::optional<int64_t>(std::nullopt)));
  }
  if (totalBytes > static_cast<int64_t>(maxBytes)) {
    PANGROVA_LOG(INFO) << "event=flight_batch_exceeds_request_budget"
                       << " requested_bytes=" << maxBytes
                       << " received_bytes=" << totalBytes;
  }
  std::vector<ContinuePromise> queuePromises;
  {
    std::lock_guard<std::mutex> queueLock(queue_->mutex());
    for (auto& page : pages) {
      queue_->enqueueLocked(std::move(page), queuePromises);
    }
  }
  for (auto& queuePromise : queuePromises) {
    queuePromise.setValue();
  }
  return Response{totalBytes, false, {}};
}

PangrovaExchangeSource::Response PangrovaExchangeSource::enqueueExternalBatch(
    std::string data,
    uint32_t maxBytes) {
  const auto bytes = static_cast<int64_t>(data.size());
  auto buffer = folly::IOBuf::copyBuffer(data);
  auto page = std::make_unique<SerializedPage>(
      std::move(buffer), nullptr, std::optional<int64_t>(std::nullopt));
  std::vector<ContinuePromise> queuePromises;
  {
    std::lock_guard<std::mutex> queueLock(queue_->mutex());
    queue_->enqueueLocked(std::move(page), queuePromises);
  }
  for (auto& queuePromise : queuePromises) {
    queuePromise.setValue();
  }
  if (bytes > static_cast<int64_t>(maxBytes)) {
    PANGROVA_LOG(INFO) << "event=external_batch_exceeds_request_budget"
                       << " requested_bytes=" << maxBytes
                       << " received_bytes=" << bytes;
  }
  return Response{bytes, false, {}};
}

void PangrovaExchangeSource::finishFlightReadSuccess(
    const std::shared_ptr<folly::SharedPromise<Response>>& promise,
    Response response) {
  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    if (flight_read_promise_ == promise) {
      flight_read_in_flight_ = false;
      flight_read_promise_.reset();
      flight_stop_source_.reset();
    }
  }
  if (!promise->isFulfilled()) {
    promise->setValue(std::move(response));
  }
}

void PangrovaExchangeSource::finishFlightReadFailure(
    const std::shared_ptr<folly::SharedPromise<Response>>& promise,
    std::string message) {
  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    if (closed_.load()) {
      message.clear();
    } else {
      flight_terminal_error_ = message;
    }
    flight_reader_.reset();
    flight_client_.reset();
    if (flight_read_promise_ == promise) {
      flight_read_in_flight_ = false;
      flight_read_promise_.reset();
      flight_stop_source_.reset();
    }
  }
  if (promise->isFulfilled()) {
    return;
  }
  if (message.empty()) {
    promise->setValue(Response{0, false, {}});
  } else {
    promise->setException(
        folly::make_exception_wrapper<std::runtime_error>(
            std::move(message)));
  }
}

void PangrovaExchangeSource::clearRequestPending() {
  std::lock_guard<std::mutex> queueLock(queue_->mutex());
  requestPending_ = false;
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
