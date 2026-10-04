// Task-scoped exchange binding registry.
//
// Runtime commands bind plan ExchangeInput nodes to producer-issued Flight
// tickets before a Velox task starts. The runner atomically consumes a sealed
// binding and injects RemoteConnectorSplit objects. ExchangeSource instances
// then resolve their process-local source IDs here; no executor handle or
// credential is reconstructed on the consumer.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/exchange/ExternalShuffleIngress.h"

namespace pangrova::velox {

struct PendingEndpoint {
  std::string source_task_id;
  int32_t source_stage_id = 0;
  int32_t source_partition_id = 0;
  int32_t output_partition_id = 0;
  uint64_t plan_epoch = 0;
  uint32_t attempt_id = 0;
  uint64_t fencing_token = 0;
  std::string host;
  int port = 0;
  std::string tls_identity;
  std::string signed_ticket;
  uint64_t ticket_expires_at_epoch_ms = 0;
  uint64_t expected_rows = 0;
  uint64_t expected_bytes = 0;
  std::shared_ptr<ExternalShuffleIngress> external_ingress;
};

struct PendingExchangeSource {
  std::string runtime_source_id;
  PendingEndpoint endpoint;
};

struct PendingExchangeInput {
  std::string plan_node_id;
  std::string exchange_id;
  uint32_t expected_source_partitions = 0;
  std::vector<PendingExchangeSource> sources;
};

class PendingEndpointRegistry {
 public:
  static PendingEndpointRegistry& instance();

  void registerSource(
      int64_t executor_handle,
      const std::string& plan_node_id,
      const std::string& exchange_id,
      PendingEndpoint endpoint);

  void registerExternalSource(
      int64_t executor_handle,
      const std::string& plan_node_id,
      const std::string& exchange_id,
      uint64_t deadline_epoch_ms,
      size_t max_batch_bytes);

  void pushExternalBatch(
      int64_t executor_handle,
      const std::string& plan_node_id,
      const std::string& exchange_id,
      ExternalShuffleChunk chunk);

  void finishExternalSource(
      int64_t executor_handle,
      const std::string& plan_node_id,
      const std::string& exchange_id,
      int64_t last_sequence,
      const std::string& failure,
      const std::function<void()>& on_failure_accepted);

  void sealInput(
      int64_t executor_handle,
      const std::string& plan_node_id,
      const std::string& exchange_id,
      uint32_t expected_source_partitions);

  std::optional<PendingExchangeInput> takeSealedInput(
      int64_t executor_handle,
      const std::string& plan_node_id);

  std::optional<PendingEndpoint> takeEndpoint(
      const std::string& runtime_source_id);

  void discardExecutor(int64_t executor_handle);
  void clear();

 private:
  PendingEndpointRegistry() = default;

  struct SourceState {
    std::string runtime_source_id;
    PendingEndpoint endpoint;
  };

  struct InputState {
    std::string exchange_id;
    bool sealed = false;
    uint32_t expected_source_partitions = 0;
    std::unordered_map<std::string, SourceState> sources;
  };

  struct ExecutorState {
    std::unordered_map<std::string, InputState> inputs;
  };

  struct ExternalState {
    std::string exchange_id;
    std::shared_ptr<ExternalShuffleIngress> ingress;
  };

  static std::string sourceKey(const PendingEndpoint& endpoint);
  std::string nextRuntimeSourceId(int64_t executor_handle);

  std::mutex mutex_;
  std::unordered_map<int64_t, ExecutorState> executors_;
  std::unordered_map<std::string, PendingEndpoint> endpoints_;
  std::unordered_map<std::string, int64_t> endpoint_owners_;
  std::unordered_map<
      int64_t,
      std::unordered_map<std::string, ExternalState>> external_ingresses_;
  std::atomic<uint64_t> next_source_id_{1};

  std::shared_ptr<ExternalShuffleIngress> findExternalIngressLocked(
      int64_t executor_handle,
      const std::string& plan_node_id,
      const std::string& exchange_id);
};

}  // namespace pangrova::velox
