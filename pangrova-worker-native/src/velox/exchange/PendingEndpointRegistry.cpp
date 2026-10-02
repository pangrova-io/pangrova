#include "velox/exchange/PendingEndpointRegistry.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <utility>

namespace pangrova::velox {

namespace {
constexpr size_t kExternalIngressBufferBytes = 16 * 1024 * 1024;
}

PendingEndpointRegistry& PendingEndpointRegistry::instance() {
  static PendingEndpointRegistry registry;
  return registry;
}

std::string
PendingEndpointRegistry::sourceKey(const PendingEndpoint& endpoint) {
  return std::to_string(endpoint.source_stage_id) + "#" +
         std::to_string(endpoint.source_partition_id);
}

std::string
PendingEndpointRegistry::nextRuntimeSourceId(int64_t executor_handle) {
  const auto sequence = next_source_id_.fetch_add(1, std::memory_order_relaxed);
  return "pangrova://exchange-source/" + std::to_string(executor_handle) + "/" +
         std::to_string(sequence);
}

void PendingEndpointRegistry::registerSource(int64_t executor_handle,
                                             const std::string& plan_node_id,
                                             const std::string& exchange_id,
                                             PendingEndpoint endpoint) {
  if (executor_handle <= 0 || plan_node_id.empty() || exchange_id.empty()) {
    throw std::invalid_argument(
        "exchange source requires executor, plan node, and exchange identity");
  }
  if (endpoint.source_task_id.empty() || endpoint.host.empty() ||
      endpoint.port <= 0 || endpoint.signed_ticket.empty() ||
      endpoint.source_stage_id < 0 || endpoint.source_partition_id < 0 ||
      endpoint.output_partition_id < 0 || endpoint.plan_epoch == 0 ||
      endpoint.fencing_token == 0 || endpoint.ticket_expires_at_epoch_ms == 0) {
    throw std::invalid_argument(
        "exchange source requires task, endpoint, and producer-issued ticket");
  }
  const auto now_millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  if (endpoint.ticket_expires_at_epoch_ms <=
      static_cast<uint64_t>(now_millis)) {
    throw std::invalid_argument("exchange source ticket is already expired");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  auto& input = executors_[executor_handle].inputs[plan_node_id];
  if (input.exchange_id.empty()) {
    input.exchange_id = exchange_id;
  } else if (input.exchange_id != exchange_id) {
    throw std::logic_error(
        "plan node is already bound to a different exchange");
  }
  if (input.sealed) {
    throw std::logic_error("exchange input is already sealed");
  }
  if (input.sources.contains("external")) {
    throw std::logic_error(
        "exchange input cannot mix direct and external transports");
  }

  const auto key = sourceKey(endpoint);
  auto existing = input.sources.find(key);
  if (existing != input.sources.end()) {
    const auto& prior = existing->second.endpoint;
    const bool older_attempt = endpoint.plan_epoch < prior.plan_epoch ||
                               (endpoint.plan_epoch == prior.plan_epoch &&
                                endpoint.attempt_id < prior.attempt_id);
    if (older_attempt || endpoint.fencing_token < prior.fencing_token) {
      throw std::logic_error("stale exchange source attempt rejected");
    }
    const bool same_attempt = endpoint.plan_epoch == prior.plan_epoch &&
                              endpoint.fencing_token == prior.fencing_token &&
                              endpoint.attempt_id == prior.attempt_id;
    if (same_attempt) {
      const bool identical =
          endpoint.source_task_id == prior.source_task_id &&
          endpoint.source_stage_id == prior.source_stage_id &&
          endpoint.source_partition_id == prior.source_partition_id &&
          endpoint.output_partition_id == prior.output_partition_id &&
          endpoint.host == prior.host && endpoint.port == prior.port &&
          endpoint.signed_ticket == prior.signed_ticket &&
          endpoint.ticket_expires_at_epoch_ms ==
              prior.ticket_expires_at_epoch_ms &&
          endpoint.tls_identity == prior.tls_identity &&
          endpoint.expected_rows == prior.expected_rows &&
          endpoint.expected_bytes == prior.expected_bytes;
      if (!identical) {
        throw std::logic_error(
            "exchange source changed without a newer fencing token");
      }
      return;
    }
    if (endpoint.fencing_token <= prior.fencing_token) {
      throw std::logic_error(
          "newer exchange attempt requires a greater fencing token");
    }
    endpoints_.erase(existing->second.runtime_source_id);
    endpoint_owners_.erase(existing->second.runtime_source_id);
  }

  auto runtime_id = nextRuntimeSourceId(executor_handle);
  endpoints_[runtime_id] = endpoint;
  endpoint_owners_[runtime_id] = executor_handle;
  input.sources[key] = SourceState{runtime_id, std::move(endpoint)};
}

void PendingEndpointRegistry::registerExternalSource(
    int64_t executor_handle, const std::string& plan_node_id,
    const std::string& exchange_id, uint64_t deadline_epoch_ms,
    size_t max_batch_bytes) {
  if (executor_handle <= 0 || plan_node_id.empty() || exchange_id.empty() ||
      deadline_epoch_ms == 0 || max_batch_bytes == 0) {
    throw std::invalid_argument(
        "external exchange source requires identities and deadline");
  }
  auto ingress = std::make_shared<ExternalShuffleIngress>(
      std::min(kExternalIngressBufferBytes, max_batch_bytes), max_batch_bytes,
      deadline_epoch_ms);
  std::lock_guard<std::mutex> lock(mutex_);
  auto external_executor = external_ingresses_.find(executor_handle);
  if (external_executor != external_ingresses_.end() &&
      external_executor->second.contains(plan_node_id)) {
    throw std::logic_error("external exchange input is already registered");
  }
  auto& input = executors_[executor_handle].inputs[plan_node_id];
  if (!input.exchange_id.empty() || input.sealed || !input.sources.empty()) {
    throw std::logic_error("exchange input is already registered");
  }
  input.exchange_id = exchange_id;
  auto runtime_id = nextRuntimeSourceId(executor_handle);
  PendingEndpoint endpoint;
  endpoint.external_ingress = std::move(ingress);
  endpoints_[runtime_id] = endpoint;
  endpoint_owners_[runtime_id] = executor_handle;
  input.sources.emplace("external",
                        SourceState{runtime_id, std::move(endpoint)});
  external_ingresses_[executor_handle].emplace(
      plan_node_id,
      ExternalState{exchange_id,
                    input.sources.at("external").endpoint.external_ingress});
}

void PendingEndpointRegistry::pushExternalBatch(int64_t executor_handle,
                                                const std::string& plan_node_id,
                                                const std::string& exchange_id,
                                                ExternalShuffleChunk chunk) {
  std::shared_ptr<ExternalShuffleIngress> ingress;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ingress =
        findExternalIngressLocked(executor_handle, plan_node_id, exchange_id);
  }
  ingress->push(std::move(chunk));
}

void PendingEndpointRegistry::finishExternalSource(
    int64_t executor_handle, const std::string& plan_node_id,
    const std::string& exchange_id, int64_t last_sequence,
    const std::string& failure) {
  std::shared_ptr<ExternalShuffleIngress> ingress;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ingress =
        findExternalIngressLocked(executor_handle, plan_node_id, exchange_id);
  }
  if (failure.empty()) {
    ingress->finish(last_sequence);
  } else {
    ingress->fail(failure);
  }
}

std::shared_ptr<ExternalShuffleIngress>
PendingEndpointRegistry::findExternalIngressLocked(
    int64_t executor_handle, const std::string& plan_node_id,
    const std::string& exchange_id) {
  auto executor = external_ingresses_.find(executor_handle);
  if (executor == external_ingresses_.end()) {
    throw std::logic_error("external exchange executor is not registered");
  }
  auto input = executor->second.find(plan_node_id);
  if (input == executor->second.end() ||
      input->second.exchange_id != exchange_id) {
    throw std::logic_error("external exchange input identity mismatch");
  }
  if (!input->second.ingress) {
    throw std::logic_error("external exchange ingress is not registered");
  }
  return input->second.ingress;
}

void PendingEndpointRegistry::sealInput(int64_t executor_handle,
                                        const std::string& plan_node_id,
                                        const std::string& exchange_id,
                                        uint32_t expected_source_partitions) {
  if (executor_handle <= 0 || plan_node_id.empty() || exchange_id.empty() ||
      expected_source_partitions == 0) {
    throw std::invalid_argument(
        "exchange seal requires identities and a positive source count");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  auto executor = executors_.find(executor_handle);
  if (executor == executors_.end()) {
    throw std::logic_error("exchange executor has no registered sources");
  }
  auto binding = executor->second.inputs.find(plan_node_id);
  if (binding == executor->second.inputs.end()) {
    throw std::logic_error("exchange plan node has no registered sources");
  }
  auto& input = binding->second;
  if (input.exchange_id != exchange_id) {
    throw std::logic_error("exchange seal identity mismatch");
  }
  if (input.sources.size() != expected_source_partitions) {
    throw std::logic_error("exchange source cardinality mismatch: expected=" +
                           std::to_string(expected_source_partitions) +
                           " actual=" + std::to_string(input.sources.size()));
  }
  if (input.sealed &&
      input.expected_source_partitions != expected_source_partitions) {
    throw std::logic_error("exchange input was sealed with a different count");
  }
  input.expected_source_partitions = expected_source_partitions;
  input.sealed = true;
}

std::optional<PendingExchangeInput>
PendingEndpointRegistry::takeSealedInput(int64_t executor_handle,
                                         const std::string& plan_node_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto executor = executors_.find(executor_handle);
  if (executor == executors_.end()) {
    return std::nullopt;
  }
  auto binding = executor->second.inputs.find(plan_node_id);
  if (binding == executor->second.inputs.end() || !binding->second.sealed) {
    return std::nullopt;
  }

  PendingExchangeInput result;
  result.plan_node_id = plan_node_id;
  result.exchange_id = binding->second.exchange_id;
  result.expected_source_partitions =
      binding->second.expected_source_partitions;
  result.sources.reserve(binding->second.sources.size());
  for (auto& entry : binding->second.sources) {
    auto& source = entry.second;
    result.sources.push_back(
        PendingExchangeSource{source.runtime_source_id, source.endpoint});
  }
  std::sort(
      result.sources.begin(), result.sources.end(),
      [](const auto& left, const auto& right) {
        if (left.endpoint.source_stage_id != right.endpoint.source_stage_id) {
          return left.endpoint.source_stage_id < right.endpoint.source_stage_id;
        }
        return left.endpoint.source_partition_id <
               right.endpoint.source_partition_id;
      });
  executor->second.inputs.erase(binding);
  if (executor->second.inputs.empty()) {
    executors_.erase(executor);
  }
  return result;
}

std::optional<PendingEndpoint>
PendingEndpointRegistry::takeEndpoint(const std::string& runtime_source_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto endpoint = endpoints_.find(runtime_source_id);
  if (endpoint == endpoints_.end()) {
    return std::nullopt;
  }
  auto result = std::move(endpoint->second);
  endpoints_.erase(endpoint);
  endpoint_owners_.erase(runtime_source_id);
  return result;
}

void PendingEndpointRegistry::discardExecutor(int64_t executor_handle) {
  std::vector<std::shared_ptr<ExternalShuffleIngress>> ingresses;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto external = external_ingresses_.find(executor_handle);
    if (external != external_ingresses_.end()) {
      ingresses.reserve(external->second.size());
      for (const auto& [plan_node_id, state] : external->second) {
        (void)plan_node_id;
        if (state.ingress) {
          ingresses.push_back(state.ingress);
        }
      }
      external_ingresses_.erase(external);
    }
    executors_.erase(executor_handle);
    for (auto owner = endpoint_owners_.begin();
         owner != endpoint_owners_.end();) {
      if (owner->second == executor_handle) {
        endpoints_.erase(owner->first);
        owner = endpoint_owners_.erase(owner);
      } else {
        ++owner;
      }
    }
  }
  for (const auto& ingress : ingresses) {
    ingress->cancel();
  }
}

void PendingEndpointRegistry::clear() {
  std::vector<std::shared_ptr<ExternalShuffleIngress>> ingresses;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [executor_handle, inputs] : external_ingresses_) {
      (void)executor_handle;
      for (const auto& [plan_node_id, state] : inputs) {
        (void)plan_node_id;
        if (state.ingress) {
          ingresses.push_back(state.ingress);
        }
      }
    }
    executors_.clear();
    external_ingresses_.clear();
    endpoints_.clear();
    endpoint_owners_.clear();
  }
  for (const auto& ingress : ingresses) {
    ingress->cancel();
  }
}

} // namespace pangrova::velox
