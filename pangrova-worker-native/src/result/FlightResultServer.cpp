// FlightResultServer: Arrow Flight server that pulls pre-serialized
// SerializedPage from Velox's OutputBufferManager and serves them
// to the authorized client via Arrow Flight DoGet.
//
// Aligned with Prestissimo: same Velox components, Arrow Flight transport.

#include "result/FlightResultServer.h"
#include "result/ResultBatchStreamReader.h"
#include "result/ResultReadAuthority.h"
#include "capability/LogicalTypeContract.h"
#include <arrow/util/key_value_metadata.h>
#include <chrono>
#include <fstream>
#include <exception>
#include <stdexcept>
#include <unordered_set>
#include <velox/exec/OutputBufferManager.h>
#include "observability/PangrovaLog.h"

namespace pangrova::result {

// F-NATIVE-SHUF-08: Singleton instance pointer for per-instance caches.
std::atomic<FlightResultServer*> FlightResultServer::instance_{nullptr};
std::shared_mutex FlightResultServer::instance_mutex_;

FlightResultServer* FlightResultServer::instance() {
  return instance_.load(std::memory_order_acquire);
}

void FlightResultServer::registerTaskOutputType(const std::string& taskId, TaskOutputInfo info) {
  if (!info.buffers) info.buffers = facebook::velox::exec::OutputBufferManager::getInstanceRef();
  std::shared_lock<std::shared_mutex> service_lock(instance_mutex_);
  auto* server = instance();
  if (!server || !server->isRunning() || !info.output_type || !info.wire_schema || !info.pool || !info.ownership ||
      static_cast<size_t>(info.wire_schema->num_fields()) != info.output_type->size() ||
      info.column_labels.size() != info.output_type->size() || info.authority.attempt().task_id() != taskId ||
      info.authority.security().tenant_id().empty() || info.authority.deadline_epoch_ms() == 0 || info.claimed) {
    throw std::invalid_argument("Result task requires live server, exact admitted authority, schema, labels and pool");
  }
  std::lock_guard<std::mutex> lock(server->output_type_mutex_);
  if (!server->isRunning()) throw std::runtime_error("Result server is shutting down");
  if (!server->output_type_cache_.emplace(taskId, std::move(info)).second) {
    throw std::invalid_argument("Result task identity is already registered");
  }
}

void FlightResultServer::unregisterTaskOutputType(const std::string& taskId) {
  std::optional<TaskOutputInfo> removed;
  {
    std::shared_lock<std::shared_mutex> service_lock(instance_mutex_);
    auto* server = instance();
    if (!server) return;
    std::lock_guard<std::mutex> lock(server->output_type_mutex_);
    const auto found = server->output_type_cache_.find(taskId);
    if (found == server->output_type_cache_.end()) return;
    removed = std::move(found->second);
    server->output_type_cache_.erase(found);
  }
  removed->ownership->revokeAuthority();
}

std::optional<FlightResultServer::TaskOutputInfo> FlightResultServer::lookupTaskOutputInfo(const std::string& taskId) {
  std::shared_lock<std::shared_mutex> service_lock(instance_mutex_);
  auto* server = instance();
  if (!server) return std::nullopt;
  std::lock_guard<std::mutex> lock(server->output_type_mutex_);
  auto it = server->output_type_cache_.find(taskId);
  return it == server->output_type_cache_.end() ? std::nullopt : std::optional<TaskOutputInfo>(it->second);
}

arrow::Result<FlightResultServer::TaskOutputInfo> FlightResultServer::claimTaskOutputInfo(
    const protocol::v2::ResultReadGrant& grant) {
  std::shared_lock<std::shared_mutex> service_lock(instance_mutex_);
  auto* server = instance();
  if (!server) return arrow::Status::Invalid("Result authority is unavailable");
  std::lock_guard<std::mutex> lock(server->output_type_mutex_);
  auto it = server->output_type_cache_.find(grant.attempt().task_id());
  if (it == server->output_type_cache_.end() || it->second.claimed ||
      it->second.ownership->evidence().authority_revoked) {
    return arrow::Status::Invalid("Result authority is unavailable or already claimed");
  }
  ARROW_RETURN_NOT_OK(validateResultRead(grant, it->second.authority));
  it->second.claimed = true;
  return it->second;
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

arrow::Result<std::shared_ptr<arrow::Schema>> withResultColumnTypes(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<ResultColumnVectorDimension>& result_column_types) {
  if (!schema) {
    return arrow::Status::Invalid("Result schema is unavailable");
  }
  auto fields = schema->fields();
  std::unordered_set<uint32_t> seen;
  for (const auto& column : result_column_types) {
    if (column.column_index >= fields.size() || column.dimension == 0 ||
        !seen.emplace(column.column_index).second) {
      return arrow::Status::Invalid("Invalid result column logical type index or dimension");
    }
    const auto& field = fields[column.column_index];
    try {
      if (column.dimension != capability::requireStorageVectorDimension(column.logical_type)) {
        return arrow::Status::Invalid("VECTOR dimension differs from admitted logical type");
      }
      capability::validateVectorResultField(column.logical_type, *field);
    } catch (const std::invalid_argument& error) {
      return arrow::Status::TypeError("Invalid VECTOR result field: ", error.what());
    }
    auto metadata = field->metadata()
        ? field->metadata()->Copy()
        : std::make_shared<arrow::KeyValueMetadata>();
    if (metadata->FindKey("ARROW:extension:name") >= 0 ||
        metadata->FindKey("ARROW:extension:metadata") >= 0) {
      return arrow::Status::Invalid(
          "Result column already has extension metadata at index " +
          std::to_string(column.column_index));
    }
    metadata->Append("ARROW:extension:name", "pangrova.vector");
    metadata->Append("ARROW:extension:metadata",
                     std::to_string(column.dimension));
    fields[column.column_index] = field->WithMetadata(metadata);
  }
  return arrow::schema(std::move(fields), schema->metadata());
}

}  // namespace detail

FlightResultServer::FlightResultServer(FlightResultServerConfig config)
    : config_(std::move(config)) {}

FlightResultServer::~FlightResultServer() {
  try {
    stop();
  } catch (const std::exception& error) {
    PANGROVA_LOG(ERROR) << "event=flight_result_destructor_cleanup_failed error=" << error.what();
  }
}

int FlightResultServer::start() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (running_.load() || initialized_) {
    throw std::logic_error("FlightResultServer already running");
  }

  const bool any_tls_path = !config_.tls_cert_chain_path.empty() ||
      !config_.tls_private_key_path.empty() ||
      !config_.tls_root_ca_path.empty();
  auto location_result = any_tls_path
      ? arrow::flight::Location::ForGrpcTls(config_.bind_host, config_.port)
      : arrow::flight::Location::ForGrpcTcp(config_.bind_host, config_.port);
  if (!location_result.ok()) {
    throw std::runtime_error("Failed to create Flight location: " +
        location_result.status().ToString());
  }

  arrow::flight::FlightServerOptions options(*location_result);

  // Configure mutual TLS if any certificate path is provided. Partial or
  // unreadable security configuration is fatal; plaintext downgrade is never
  // an error-recovery path.
  if (any_tls_path) {
    if (config_.tls_cert_chain_path.empty() ||
        config_.tls_private_key_path.empty() ||
        config_.tls_root_ca_path.empty()) {
      throw std::invalid_argument(
          "Flight result mutual TLS requires certificate chain, private key and root CA");
    }
    std::ifstream cert_file(config_.tls_cert_chain_path);
    std::ifstream key_file(config_.tls_private_key_path);
    if (!cert_file.good() || !key_file.good()) {
      throw std::runtime_error(
          "Failed to read Flight result TLS certificate or private key");
    }
    std::ifstream ca_file(config_.tls_root_ca_path);
    if (!ca_file.good()) {
      throw std::runtime_error("Failed to read Flight result TLS root CA");
    }
    std::string cert_pem((std::istreambuf_iterator<char>(cert_file)),
                          std::istreambuf_iterator<char>());
    std::string key_pem((std::istreambuf_iterator<char>(key_file)),
                         std::istreambuf_iterator<char>());
    std::string ca_pem((std::istreambuf_iterator<char>(ca_file)),
                        std::istreambuf_iterator<char>());
    options.tls_certificates.push_back(
        arrow::flight::CertKeyPair{cert_pem, key_pem});
    options.root_certificates = ca_pem;
    options.verify_client = true;
    PANGROVA_LOG(INFO) << "event=flight_result_server_mtls_enabled"
                      << " port=" << config_.port;
  } else {
    PANGROVA_LOG(WARNING) << "event=flight_result_server_no_tls"
                        << " port=" << config_.port
                        << " detail=binding_without_tls_scoped_ticket_required";
  }

  auto init_status = Init(options);
  if (!init_status.ok()) {
    throw std::runtime_error("Failed to init Flight result server: " + init_status.ToString());
  }
  initialized_ = true;
  transport_stopped_ = false;

  bound_port_ = port();
  running_.store(true);
  {
    std::unique_lock<std::shared_mutex> service_lock(instance_mutex_);
    instance_.store(this, std::memory_order_release);
  }

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
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (!initialized_) return;
  running_.store(false);
  {
    std::unique_lock<std::shared_mutex> service_lock(instance_mutex_);
    auto* expected = this;
    instance_.compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel);
  }
  std::unordered_map<std::string, TaskOutputInfo> revoked;
  {
    std::lock_guard<std::mutex> lock(output_type_mutex_);
    revoked.swap(output_type_cache_);
  }
  // Transfer the actual cache nodes, not an unbudgeted deep copy of every
  // schema/security envelope. Admission is sealed; failed nodes are returned
  // to the same authoritative cache for retry without a second registry.
  for (const auto& entry : revoked) entry.second.ownership->revokeAuthority();
  std::exception_ptr cleanup_failure;
  for (auto current = revoked.begin(); current != revoked.end();) {
    try {
      current->second.buffers->deleteResults(current->first, 0);
      current->second.ownership->revokeAndClearQueue();
      current = revoked.erase(current);
    } catch (...) {
      if (!cleanup_failure) cleanup_failure = std::current_exception();
      ++current;
    }
  }
  {
    std::lock_guard<std::mutex> lock(output_type_mutex_);
    output_type_cache_.swap(revoked);
  }

  // DoGet may be blocked in an SDK fetch whose route disappeared on failure.
  // Wake those exact readers before gRPC waits for their exit.
  if (!transport_stopped_) {
    // Revocation has already made every in-flight read invalid. A graceful
    // Flight shutdown can wait forever for an idle client to close its RPC;
    // cancel remaining transport calls after the revocation barrier instead.
    const auto deadline = std::chrono::system_clock::now();
    const auto status = Shutdown(&deadline);
    if (!status.ok()) throw std::runtime_error("Flight result shutdown failed: " + status.ToString());
    if (server_thread_.joinable()) server_thread_.join();
    transport_stopped_ = true;
  }
  if (cleanup_failure) std::rethrow_exception(cleanup_failure);
  initialized_ = false;

  PANGROVA_LOG(INFO) << "event=flight_result_server_stopped"
                    << " port=" << bound_port_;
}

arrow::Status FlightResultServer::DoGet(
    const arrow::flight::ServerCallContext& context, const arrow::flight::Ticket& request,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
  auto authenticated = authenticateResultRead(request.ticket);
  if (!authenticated.ok()) return arrow::flight::MakeFlightError(
      arrow::flight::FlightStatusCode::Unauthenticated, "Invalid result authority");
  const auto& grant = *authenticated;
  const auto& taskId = grant.attempt().task_id();
  const auto& buffers = facebook::velox::exec::OutputBufferManager::getInstanceRef();
  if (!buffers) return arrow::Status::Invalid("Result buffer service is unavailable");
  auto info = lookupTaskOutputInfo(taskId);
  auto stats = buffers->stats(taskId);
  for (int retry = 0; retry < 25 && (!info || !stats); ++retry) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10 * (1 + retry / 5)));
    info = lookupTaskOutputInfo(taskId);
    stats = buffers->stats(taskId);
  }
  if (!info || !stats || !validateResultRead(grant, info->authority).ok()) {
    return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthorized,
        "Result authority has no matching live registration");
  }
  ARROW_ASSIGN_OR_RAISE(auto claimed, claimTaskOutputInfo(grant));
  ARROW_ASSIGN_OR_RAISE(auto reader, resultBatchStreamReader(std::move(claimed), grant,
      config_.max_bytes_per_response, config_.result_fetch_timeout_seconds, &context));
  *stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
  return arrow::Status::OK();
}

}  // namespace pangrova::result
