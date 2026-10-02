// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Command dispatcher: routes canonical v2 RuntimeCommand messages.
// Each handler is a standalone function — single responsibility per command.

#include "jni/command/NativeCommandDispatcher.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include "jni/RuntimeInitializer.h"
#include "jni/bridge/JniBridge.h"
#include "jni/bridge/NativeDatasetControl.h"
#include "jni/command/FlightServerRegistry.h"
#include "jni/command/NativeResourceDrainControl.h"
#include "jni/registry/HandleRegistry.h"
#include "memory/MemoryPressure.h"
#include "observability/PangrovaLog.h"
#include "split/PathUtils.h"
#include "udf/NativeUdfRegistry.h"
#if PANGROVA_HAS_FLIGHT
#include "result/FlightResultServer.h"
#include "shuffle/FlightShuffleServer.h"
#include "shuffle/ShuffleTicketKeys.h"
#include "shuffle/TicketSigner.h"
#endif
#include "vector/diskann/DiskAnnIndexCache.h"
#include "velox/exchange/PendingEndpointRegistry.h"
#include "velox/memory/VeloxMemoryGovernor.h"

// Velox AsyncDataCache for cache operations.
#include "artifact/ImmutableArtifactCache.h"
#include "cache/SemanticResultCache.h"
#include "capability/NativeCapabilityCatalog.h"
#include "compat/NativeFeatureDetection.h"

#include "google/protobuf/descriptor.pb.h"
#include "pangrova/protocol/v2/capability.pb.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni {
namespace {

namespace proto = pangrova::protocol::v2;

// Serialize a RuntimeCommandResponse to bytes.
std::vector<uint8_t>
serializeResponse(const proto::RuntimeCommandResponse& resp) {
  std::vector<uint8_t> out(resp.ByteSizeLong());
  resp.SerializeToArray(out.data(), static_cast<int>(out.size()));
  return out;
}

// Build an error response with the given message.
proto::RuntimeCommandResponse
errorResponse(const std::string& message,
              proto::ErrorClass error_class = proto::ERROR_CLASS_INTERNAL,
              std::string code = "NATIVE_COMMAND_FAILED") {
  proto::RuntimeCommandResponse resp;
  resp.set_success(false);
  auto* error = resp.mutable_error();
  error->set_error_class(error_class);
  error->set_code(std::move(code));
  error->set_message(message);
  error->set_retryable(false);
  error->set_failed_component("native-command-dispatcher");
  return resp;
}

// Build a success response (no result payload).
proto::RuntimeCommandResponse successResponse() {
  proto::RuntimeCommandResponse resp;
  resp.set_success(true);
  return resp;
}

template <typename Message>
bool unpackPayload(const proto::RuntimeCommand& command, Message* payload,
                   proto::RuntimeCommandResponse& response,
                   const char* expected_type) {
  if (!command.has_payload() || !command.payload().UnpackTo(payload)) {
    response = errorResponse(
        std::string("Command payload must be ") + expected_type,
        proto::ERROR_CLASS_INVALID_REQUEST, "INVALID_COMMAND_PAYLOAD");
    return false;
  }
  return true;
}

template <typename Message>
void setResponsePayload(proto::RuntimeCommandResponse& response,
                        const Message& payload) {
  response = successResponse();
  response.mutable_payload()->PackFrom(payload);
}

void collectDescriptorFiles(
    const google::protobuf::FileDescriptor* descriptor,
    std::map<std::string, const google::protobuf::FileDescriptor*>& files) {
  if (!descriptor || !files.emplace(descriptor->name(), descriptor).second) {
    return;
  }
  for (int i = 0; i < descriptor->dependency_count(); ++i) {
    collectDescriptorFiles(descriptor->dependency(i), files);
  }
}

void clearDerivedFields(google::protobuf::DescriptorProto* message) {
  for (auto& field : *message->mutable_field()) {
    field.clear_json_name();
  }
  for (auto& extension : *message->mutable_extension()) {
    extension.clear_json_name();
  }
  for (auto& nested : *message->mutable_nested_type()) {
    clearDerivedFields(&nested);
  }
}

void normalizeDescriptor(google::protobuf::FileDescriptorProto* descriptor) {
  for (auto& message : *descriptor->mutable_message_type()) {
    clearDerivedFields(&message);
  }
  for (auto& extension : *descriptor->mutable_extension()) {
    extension.clear_json_name();
  }
}

std::string canonicalDescriptorSet(const google::protobuf::FileDescriptor* root,
                                   bool normalize_derived_fields = false) {
  std::map<std::string, const google::protobuf::FileDescriptor*> files;
  collectDescriptorFiles(root, files);
  google::protobuf::FileDescriptorSet descriptor_set;
  for (const auto& [name, descriptor] : files) {
    (void)name;
    auto* output = descriptor_set.add_file();
    descriptor->CopyTo(output);
    if (normalize_derived_fields) {
      normalizeDescriptor(output);
    }
  }
  return descriptor_set.SerializeAsString();
}

std::string sha256(const std::string& input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int digest_size = 0;
  if (EVP_Digest(input.data(), input.size(), digest.data(), &digest_size,
                 EVP_sha256(), nullptr) != 1 ||
      digest_size != 32) {
    throw std::runtime_error("Failed to compute canonical protocol SHA-256");
  }
  return std::string(reinterpret_cast<const char*>(digest.data()), digest_size);
}

bool constantTimeEqual(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         CRYPTO_memcmp(left.data(), right.data(), left.size()) == 0;
}

size_t externalShuffleMaxBatchBytes(
    const pangrova::executor::TaskExecutor& executor) {
  constexpr uint64_t kNativeCopiesPerBatch = 4;
  // Hand-built local/test executors have no admitted resource grant. Production
  // TaskRequest parsing requires a positive grant before an executor exists.
  constexpr size_t kDefaultUnboundExecutorBatchBudgetBytes = 64 * 1024 * 1024;
  const auto memory_limit = executor.nativeMemoryLimitBytes();
  if (memory_limit <= 0) {
    return std::min(
        kDefaultUnboundExecutorBatchBudgetBytes,
        pangrova::velox::kExternalShuffleMaxBatchBytes);
  }
  const auto bounded =
      static_cast<uint64_t>(memory_limit) / kNativeCopiesPerBatch;
  return static_cast<size_t>(std::min<uint64_t>(
      bounded,
      std::min<uint64_t>(
          std::numeric_limits<size_t>::max(),
          pangrova::velox::kExternalShuffleMaxBatchBytes)));
}

bool validateAttemptIdentity(const proto::RuntimeCommand& command,
                             const pangrova::executor::TaskExecutor& executor,
                             proto::RuntimeCommandResponse& response) {
  if (!command.has_attempt() || !command.attempt().has_execution_owner()) {
    response = errorResponse(
        "Task-scoped command requires a canonical attempt identity",
        proto::ERROR_CLASS_INVALID_REQUEST, "MISSING_COMMAND_ATTEMPT");
    return false;
  }
  const auto& attempt = command.attempt();
  const auto& owner = attempt.execution_owner();
  const bool matches =
      attempt.query_or_job_id() == executor.queryId() &&
      attempt.stage_id() == executor.stageId() &&
      attempt.partition_id() == executor.partitionId() &&
      attempt.plan_epoch() == executor.planEpoch() &&
      attempt.attempt_id() == executor.attemptId() &&
      attempt.fencing_token() == executor.fencingToken() &&
      attempt.task_id() == executor.taskId() &&
      attempt.control_epoch() == executor.controlEpoch() &&
      owner.owner_id() == executor.ownerId() &&
      owner.owner_incarnation() == executor.ownerIncarnation() &&
      owner.fencing_token() == executor.ownerFencingToken();
  if (!matches) {
    response = errorResponse(
        "Task-scoped command attempt identity does not match the executor",
        proto::ERROR_CLASS_POLICY_DENIED, "COMMAND_ATTEMPT_FENCE_MISMATCH");
    return false;
  }
  return true;
}

std::string
validateExternalEndpointCoverage(const proto::ExternalShuffleInput& external) {
  std::vector<std::pair<int32_t, int32_t>> ranges;
  ranges.reserve(external.endpoints_size());
  for (const auto& endpoint : external.endpoints()) {
    if (endpoint.host().empty() || endpoint.port() <= 0 ||
        endpoint.partition_start() < 0 ||
        endpoint.partition_end() <= endpoint.partition_start()) {
      return "External shuffle endpoint is invalid";
    }
    ranges.emplace_back(endpoint.partition_start(), endpoint.partition_end());
  }
  std::sort(ranges.begin(), ranges.end());
  int32_t next_partition = 0;
  for (const auto& [start, end] : ranges) {
    if (start != next_partition) {
      return "External shuffle endpoint ranges must be gap-free and unique";
    }
    next_partition = end;
  }
  if (next_partition < 0 ||
      static_cast<uint32_t>(next_partition) != external.partition_count()) {
    return "External shuffle endpoint ranges do not cover all partitions";
  }
  return {};
}

std::string
validateExternalSource(const proto::ExternalShuffleInput& external,
                       const pangrova::executor::TaskExecutor& executor) {
  if (external.shuffle_id().empty() || external.partition_count() == 0 ||
      external.partition_count() >
          static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) ||
      external.output_partition_id() < 0 ||
      external.output_partition_id() >=
          static_cast<int64_t>(external.partition_count()) ||
      external.plan_epoch() == 0 || !external.has_execution_owner()) {
    return "External shuffle source is missing required identity fields";
  }
  const auto& owner = external.execution_owner();
  if (external.plan_epoch() != executor.planEpoch() ||
      external.output_partition_id() != executor.partitionId() ||
      owner.owner_id() != executor.ownerId() ||
      owner.owner_incarnation() != executor.ownerIncarnation() ||
      owner.fencing_token() != executor.ownerFencingToken()) {
    return "External shuffle source identity does not match the executor";
  }
  const auto now_millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  if (external.deadline_epoch_ms() <= static_cast<uint64_t>(now_millis) ||
      (executor.deadlineEpochMs() > 0 &&
       external.deadline_epoch_ms() >
           static_cast<uint64_t>(executor.deadlineEpochMs()))) {
    return "External shuffle source deadline is invalid";
  }
  if (!external.has_read_capability() ||
      external.read_capability().token().empty() ||
      external.read_capability().expires_at_epoch_ms() <
          external.deadline_epoch_ms()) {
    return "External shuffle read capability does not cover the task deadline";
  }
  if (!external.has_commit_address() ||
      external.commit_address().host().empty() ||
      external.commit_address().port() <= 0 ||
      external.expected_producer_partitions() == 0 ||
      !external.has_expected_producer_set_checksum() ||
      external.expected_producer_set_checksum().algorithm() !=
          proto::Checksum::ALGORITHM_SHA256 ||
      external.expected_producer_set_checksum().digest().size() != 32) {
    return "External shuffle source is missing service or producer metadata";
  }
  return validateExternalEndpointCoverage(external);
}

std::string formatExternalFailure(const proto::ExecutionError& failure) {
  std::ostringstream message;
  message << "code=" << failure.code()
          << " component=" << failure.failed_component()
          << " retryable=" << (failure.retryable() ? "true" : "false")
          << " message=" << failure.message();
  return message.str();
}

// --- Per-command handlers ---

void handleInjectRuntimeFilter(const proto::RuntimeCommand& cmd,
                               int64_t executor_handle,
                               proto::RuntimeCommandResponse& resp) {
  proto::InjectRuntimeFilterRequest payload;
  if (!unpackPayload(cmd, &payload, resp,
                     "pangrova.protocol.v2.InjectRuntimeFilterRequest")) {
    return;
  }

  std::shared_ptr<pangrova::executor::TaskExecutor> executor;
  try {
    executor = HandleRegistry::instance().acquire(executor_handle);
  } catch (const std::runtime_error& e) {
    resp = errorResponse(e.what());
    return;
  }

  // Serialize the full payload back to string for TaskExecutor to parse.
  // TaskExecutor::injectDynamicFilter handles the proto → Velox Filter
  // conversion via DynamicFilterConverter.
  std::string payload_bytes;
  payload.SerializeToString(&payload_bytes);
  executor->injectDynamicFilter(payload.plan_node_id(), payload_bytes);
  resp = successResponse();
}

void handleUpdateResourceGrant(const proto::RuntimeCommand& cmd,
                               int64_t executor_handle,
                               proto::RuntimeCommandResponse& resp) {
  proto::UpdateResourceGrantRequest payload;
  if (!unpackPayload(cmd, &payload, resp,
                     "pangrova.protocol.v2.UpdateResourceGrantRequest")) {
    return;
  }
  if (!payload.has_resource_grant() ||
      payload.resource_grant().native_memory_bytes() == 0) {
    resp =
        errorResponse("Resource grant requires positive native_memory_bytes");
    return;
  }

  std::shared_ptr<pangrova::executor::TaskExecutor> executor;
  try {
    executor = HandleRegistry::instance().acquire(executor_handle);
  } catch (const std::runtime_error& e) {
    resp = errorResponse(e.what());
    return;
  }

  executor->updateMemoryLimit(
      static_cast<int64_t>(payload.resource_grant().native_memory_bytes()));
  resp = successResponse();
}

void handlePrefetchArtifacts(const proto::RuntimeCommand& cmd,
                             proto::RuntimeCommandResponse& resp) {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE && PANGROVA_HAS_VELOX_FILE &&               \
    PANGROVA_HAS_FOLLY_EXECUTOR
  proto::PrefetchArtifactsRequest payload;
  if (!unpackPayload(cmd, &payload, resp,
                     "pangrova.protocol.v2.PrefetchArtifactsRequest")) {
    return;
  }

  auto* cache = globalAsyncDataCache();
  if (!cache) {
    // Cache disabled — silently ignore prefetch requests.
    resp = successResponse();
    return;
  }

  auto executor = globalIoExecutor();
  if (!executor) {
    resp = successResponse();
    return;
  }

  // Submit async prefetch tasks for each canonical split location.
  // Each task reads the file footer region to warm the OS page cache
  // and Velox AsyncDataCache. The handler returns immediately.
  for (const auto& split : payload.splits()) {
    auto filePath = split.opaque_location();
    if (filePath.empty()) {
      continue;
    }

    filePath = pangrova::split::normalizeToS3Uri(
        pangrova::split::stripFilePrefix(filePath));

    // Async non-blocking prefetch: read footer region to warm cache.
    executor->add([filePath]() {
      try {
        auto fileSystem =
            facebook::velox::filesystems::getFileSystem(filePath, nullptr);
        if (!fileSystem) {
          return;
        }
        auto readFile = fileSystem->openFileForRead(filePath);
        auto fileSize = readFile->size();
        if (fileSize == 0) {
          return;
        }

        // Read the last min(64KB, fileSize) bytes — covers the Parquet
        // footer and column statistics for most files.
        constexpr int64_t kPrefetchFooterBytes = 64 * 1024;
        auto readSize =
            std::min(static_cast<int64_t>(fileSize), kPrefetchFooterBytes);
        auto offset = static_cast<int64_t>(fileSize) - readSize;

        std::string buffer(readSize, '\0');
        readFile->pread(offset, readSize, buffer.data());
        // Data is now in OS page cache. When Velox reads this region
        // through CachedBufferedInput, it will be served from cache.
      } catch (...) {
        // Silent ignore — prefetch failure must not affect query execution.
      }
    });
  }

  resp = successResponse();
#else
  (void)cmd;
  resp = successResponse();
#endif
}

void handleEvictCache(const proto::RuntimeCommand& cmd,
                      proto::RuntimeCommandResponse& resp) {
  proto::EvictCacheRequest payload;
  if (!unpackPayload(cmd, &payload, resp,
                     "pangrova.protocol.v2.EvictCacheRequest")) {
    return;
  }

  if (payload.pattern().empty()) {
    resp = successResponse();
    return;
  }

  // P2-BUG-11: Velox AsyncDataCache does not support pattern-based eviction.
  // Only accept wildcard "*" as an explicit "clear all" intent.
  // For any other pattern, return an error to prevent silent full-cache wipe.
  if (payload.pattern() != "*") {
    resp =
        errorResponse("Pattern-based cache eviction is not supported. "
                      "Use pattern='*' to explicitly clear all cache entries. "
                      "Received pattern='" +
                      payload.pattern() + "'");
    return;
  }

  std::cerr << "level=INFO event=cache_evict_full_clear"
            << " pattern=" << payload.pattern() << std::endl;

  cache::SemanticResultCache::instance().invalidateAll();
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
  if (auto* cache = globalAsyncDataCache()) {
    cache->clear();
  }
#endif
  resp = successResponse();
}

void handleGetMetrics(const proto::RuntimeCommand& /*cmd*/,
                      proto::RuntimeCommandResponse& resp) {
  proto::NativeMetricsSnapshot metrics;
  metrics.set_schema_version(1);
  auto* bridge = globalVeloxMemoryGovernor();
  if (bridge) {
    auto snapshot = bridge->getMemorySnapshot();
    metrics.mutable_resource_usage()->set_peak_native_memory_bytes(
        static_cast<uint64_t>(std::max<int64_t>(snapshot.peak_used_bytes, 0)));
    metrics.mutable_resource_usage()->set_spill_bytes(
        static_cast<uint64_t>(std::max<int64_t>(snapshot.spill_bytes, 0)));
  }
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
  auto* cache = globalAsyncDataCache();
  if (cache) {
    auto stats = cache->refreshStats();
    auto* cache_metrics = metrics.mutable_cache();
    cache_metrics->set_ram_hits(stats.numHit);
    cache_metrics->set_ram_misses(stats.numNew);
    cache_metrics->set_ssd_hits(stats.ssdStats ? stats.ssdStats->entriesRead
                                               : 0);
    cache_metrics->set_ssd_misses(
        stats.ssdStats ? stats.ssdStats->entriesWritten : 0);
    cache_metrics->set_promotions(stats.ssdStats ? stats.ssdStats->entriesRead
                                                 : 0);
    cache_metrics->set_demotions(stats.numEvict);
    cache_metrics->set_evictions(stats.numEvict);
    cache_metrics->set_ram_used_bytes(stats.tinySize + stats.largeSize);
    cache_metrics->set_ssd_used_bytes(
        stats.ssdStats ? stats.ssdStats->bytesWritten : 0);
    const auto total = stats.numHit + stats.numNew;
    cache_metrics->set_prefetch_hit_rate(
        total > 0
            ? static_cast<double>(stats.numHit) / static_cast<double>(total)
            : 0.0);
  }
#endif
  setResponsePayload(resp, metrics);
}

void handleMemoryPressure(const proto::RuntimeCommand& cmd,
                          proto::RuntimeCommandResponse& resp) {
  proto::MemoryPressureRequest payload;
  if (!unpackPayload(cmd, &payload, resp,
                     "pangrova.protocol.v2.MemoryPressureRequest")) {
    return;
  }

  auto* bridge = globalVeloxMemoryGovernor();
  if (!bridge) {
    resp = errorResponse("VeloxMemoryGovernor not initialized");
    return;
  }

  // Map proto level to usage percentage for signalMemoryPressure().
  double usage_percent = 0.0;
  switch (payload.level()) {
  case proto::MEMORY_PRESSURE_LEVEL_HIGH:
    usage_percent = pangrova::memory::kPressureThresholdHigh + 1.0;
    break;
  case proto::MEMORY_PRESSURE_LEVEL_CRITICAL:
    usage_percent = pangrova::memory::kPressureThresholdCritical + 1.0;
    break;
  default:
    // NONE/LOW/MEDIUM: no cache eviction needed.
    resp = successResponse();
    return;
  }

  auto* cache = globalAsyncDataCache();
  bridge->signalMemoryPressure(cache, usage_percent);

  PANGROVA_LOG(INFO) << "event=memory_pressure_applied"
                     << " pressure_level=" << static_cast<int>(payload.level())
                     << " usage_percent=" << usage_percent;

  resp = successResponse();
}

void handleGetCapabilities(const proto::RuntimeCommand& /*cmd*/,
                           proto::RuntimeCommandResponse& resp) {
  const auto descriptor_set_v2 =
      canonicalDescriptorSet(proto::RuntimeCommand::descriptor()->file(), true);
  const auto descriptor_set_v2_sha256 = sha256(descriptor_set_v2);
  proto::CapabilityResponse capabilities;
  auto* inventory_v2 = capabilities.mutable_inventory();
  auto* protocol_v2 = inventory_v2->mutable_protocol();
  protocol_v2->set_major(2);
  protocol_v2->set_minor(0);
  protocol_v2->set_descriptor_set_sha256(descriptor_set_v2_sha256);
  protocol_v2->set_rollout_epoch("ai-native-function-resource-r3");
  *inventory_v2->add_capabilities() =
      pangrova::capability::exactTopKRetrievalDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::rrfRetrievalMergeDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::linearRetrievalMergeDescriptor();
  auto* udf_registry = pangrova::jni::globalUdfRegistry();
  if (udf_registry && udf_registry->findScalar(std::string(
                          pangrova::capability::kGovernedRerankFunction))) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::crossEncoderRerankDescriptor();
  }
  if (udf_registry && udf_registry->findScalar("ai_complete")) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::aiTextGenerationDescriptor();
  }
  if (udf_registry && udf_registry->findScalar("ai_describe_image")) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::aiMultimodalGenerationDescriptor();
  }
  if (udf_registry && udf_registry->findScalar("ai_embed")) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::aiEmbeddingDescriptor();
  }
  *inventory_v2->add_capabilities() =
      pangrova::capability::diskAnnArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::hnswArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::ivfPqArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::bm25ArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::generatedValuesArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::tabularExportDescriptor();
  // Retrieval is advertised only when the node-local immutable artifact
  // substrate is actually ready. The scheduler therefore never routes a
  // DiskANN task to a Worker that merely contains the software implementation
  // but cannot satisfy its storage contract.
  if (pangrova::artifact::ImmutableArtifactCache::instance().enabled() &&
      pangrova::vector::diskann::DiskAnnIndexCache::instance().enabled()) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::diskAnnRetrievalDescriptor();
    *inventory_v2->add_capabilities() =
        pangrova::capability::hnswRetrievalDescriptor();
    *inventory_v2->add_capabilities() =
        pangrova::capability::ivfPqRetrievalDescriptor();
    *inventory_v2->add_capabilities() =
        pangrova::capability::bm25RetrievalDescriptor();
  }
  // The generated-value overlay only needs checksum-verified immutable object
  // materialization. It must not inherit the independent ANN parsed-cache
  // readiness gate.
  if (pangrova::artifact::ImmutableArtifactCache::instance().enabled()) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::generatedValuesOverlayDescriptor();
  }
  inventory_v2->add_supported_relation_type_urls(
      pangrova::capability::kRelationDetailEnvelopeTypeUrl);
  inventory_v2->set_candidate_schema_version(
      pangrova::capability::kRetrievalCandidateSchemaVersion);
  inventory_v2->set_result_envelope_version(1);
  inventory_v2->set_native_engine_version("0.1.0");
  for (const auto* rel :
       {"ReadRel", "FilterRel", "ProjectRel", "AggregateRel", "HashJoinRel",
        "MergeJoinRel", "JoinRel", "ExchangeRel", "ExtensionLeafRel",
        "SortRel", "FetchRel",
        "ConsistentPartitionWindowRel", "SetRel", "CrossRel",
        "ExtensionSingleRel", "ExtensionMultiRel"}) {
    inventory_v2->add_supported_substrait_rel_types(rel);
  }
  for (const auto* expression : {"FieldReference", "Literal", "ScalarFunction",
                                 "Cast", "IfThen", "WindowFunction"}) {
    inventory_v2->add_supported_substrait_expression_types(expression);
  }
  setResponsePayload(resp, capabilities);
}

void handleStartResultServer(const proto::RuntimeCommand& cmd,
                             proto::RuntimeCommandResponse& resp) {
#if PANGROVA_HAS_FLIGHT
  proto::StartResultServerRequest payload;
  if (!unpackPayload(cmd, &payload, resp,
                     "pangrova.protocol.v2.StartResultServerRequest")) {
    return;
  }

  auto& registry = FlightServerRegistry::instance();
  auto* existing = registry.resultServer();
  if (existing && existing->isRunning()) {
    // Already running — return the existing bound port.
    proto::StartResultServerResponse result;
    result.set_bound_port(existing->boundPort());
    setResponsePayload(resp, result);
    return;
  }

  pangrova::result::FlightResultServerConfig config;
  config.port = payload.preferred_port();
  registry.applyTransportSecurity(config);

  auto server = std::make_unique<pangrova::result::FlightResultServer>(config);
  int bound_port = server->start();
  registry.setResultServer(std::move(server));

  proto::StartResultServerResponse result;
  result.set_bound_port(bound_port);
  setResponsePayload(resp, result);
#else
  (void)cmd;
  resp = errorResponse(
      "Arrow Flight not available — built without PANGROVA_HAS_FLIGHT");
#endif
}

void handleStopResultServer(const proto::RuntimeCommand& /*cmd*/,
                            proto::RuntimeCommandResponse& resp) {
#if PANGROVA_HAS_FLIGHT
  FlightServerRegistry::instance().shutdownResultServer();
  resp = successResponse();
#else
  resp = errorResponse(
      "Arrow Flight not available — built without PANGROVA_HAS_FLIGHT");
#endif
}

void handleStartShuffleServer(const proto::RuntimeCommand& cmd,
                              int64_t executor_handle,
                              proto::RuntimeCommandResponse& resp) {
#if PANGROVA_HAS_FLIGHT
  proto::StartShuffleServerRequest request;
  if (!unpackPayload(cmd, &request, resp,
                     "pangrova.protocol.v2.StartShuffleServerRequest")) {
    return;
  }
  if (request.partition_count() == 0) {
    resp = errorResponse("START_FLIGHT_SERVER requires partition_count > 0");
    return;
  }
  auto handle = executor_handle;
  if (handle == 0) {
    resp = errorResponse(
        "START_FLIGHT_SERVER requires a non-zero executor_handle");
    return;
  }

  std::shared_ptr<pangrova::executor::TaskExecutor> executor;
  try {
    executor = HandleRegistry::instance().acquire(handle);
  } catch (const std::runtime_error& e) {
    resp = errorResponse(e.what());
    return;
  }

  auto* queue = executor->serializedShuffleQueue();
  if (!queue) {
    resp = errorResponse("Executor has no shuffle queue (not a shuffle task)");
    return;
  }
  if (executor->usesExternalShuffle()) {
    resp = errorResponse(
        "External shuffle task cannot start the Worker Flight shuffle server");
    return;
  }
  if (executor->shufflePartitionCount() > 0 &&
      executor->shufflePartitionCount() !=
          static_cast<int32_t>(request.partition_count())) {
    resp = errorResponse(
        "Shuffle partition count does not match the task binding");
    return;
  }

  auto& keys = pangrova::shuffle::ShuffleTicketKeys::instance();
  if (!keys.enabled()) {
    resp =
        errorResponse("Native shuffle ticket signing key is not initialized");
    return;
  }

  constexpr int64_t kTicketDeadlineGraceMillis = 60'000;
  constexpr int64_t kMaxTicketTtlMillis = 24LL * 60 * 60 * 1000;
  int64_t ttl_millis = request.ticket_ttl_millis() > 0
                           ? static_cast<int64_t>(request.ticket_ttl_millis())
                           : pangrova::shuffle::kDefaultTicketTtlMillis;
  const auto now_millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  if (executor->deadlineEpochMs() > 0) {
    if (executor->deadlineEpochMs() <= now_millis) {
      resp = errorResponse("Cannot mint shuffle tickets for an expired task");
      return;
    }
    const auto required_ttl =
        executor->deadlineEpochMs() - now_millis + kTicketDeadlineGraceMillis;
    ttl_millis = std::max(ttl_millis, required_ttl);
  }
  if (ttl_millis <= 0 || ttl_millis > kMaxTicketTtlMillis) {
    resp = errorResponse("Shuffle ticket TTL must be in (0, 24h]");
    return;
  }

  // Register this handle with the shared multiplexed shuffle server.
  // The server is auto-started on first registration and all handles
  // share a single port, eliminating per-handle port exhaustion.
  auto& registry = FlightServerRegistry::instance();
  registry.registerShuffleHandle(
      handle, queue, executor->queryId(), executor->stageId(),
      static_cast<int32_t>(request.partition_count()), executor->planEpoch(),
      executor->attemptId(), executor->fencingToken());
  int bound_port = registry.ensureShuffleServerRunning();

  pangrova::shuffle::TicketSigner signer(keys.currentKey(), ttl_millis);

  proto::StartShuffleServerResponse result;
  result.set_bound_port(bound_port);
  for (uint32_t partition = 0; partition < request.partition_count();
       ++partition) {
    std::ostringstream payload;
    payload << handle << "\n"
            << executor->queryId() << "\n"
            << executor->stageId() << "\n"
            << partition << "\n"
            << executor->planEpoch() << "\n"
            << executor->attemptId() << "\n"
            << executor->fencingToken();
    auto* ticket = result.add_partition_tickets();
    ticket->set_partition_id(static_cast<int32_t>(partition));
    auto signed_ticket = signer.signWithMetadata(payload.str());
    ticket->set_signed_ticket(std::move(signed_ticket.ticket));
    ticket->set_expires_at_epoch_ms(
        static_cast<uint64_t>(signed_ticket.expires_at_epoch_ms));
  }
  setResponsePayload(resp, result);
#else
  (void)cmd;
  resp = errorResponse(
      "Arrow Flight not available — built without PANGROVA_HAS_FLIGHT");
#endif
}

// Unregisters the executor handle from the shared multiplexed shuffle server.
// Called from Java when a shuffle task completes or fails, releasing the
// handle's demuxer resources while the shared server continues serving
// other handles.
void handleStopShuffleServer(const proto::RuntimeCommand& /*cmd*/,
                             int64_t executor_handle,
                             proto::RuntimeCommandResponse& resp) {
#if PANGROVA_HAS_FLIGHT
  auto handle = executor_handle;
  if (handle == 0) {
    resp = errorResponse(
        "STOP_SHUFFLE_SERVER requires a non-zero executor_handle");
    return;
  }

  FlightServerRegistry::instance().unregisterShuffleHandle(handle);

  PANGROVA_LOG(INFO) << "event=shuffle_handle_unregistered"
                     << " executor_handle=" << handle;
  resp = successResponse();
#else
  (void)cmd;
  resp = errorResponse(
      "Arrow Flight not available — built without PANGROVA_HAS_FLIGHT");
#endif
}

// Signals the native engine that no more exchange sources will be added
// for the given executor. Velox ExchangeNode uses this to know when all
// upstream data has been accounted for, preventing indefinite waits when
// the source count is zero or all sources have been registered.
void handleNoMoreExchangeSources(const proto::RuntimeCommand& cmd,
                                 int64_t executor_handle,
                                 proto::RuntimeCommandResponse& resp) {
  auto handle = executor_handle;
  if (handle == 0) {
    resp = errorResponse(
        "NO_MORE_EXCHANGE_SOURCES requires a non-zero executor_handle");
    return;
  }

  proto::NoMoreExchangeSourcesRequest request;
  if (!unpackPayload(cmd, &request, resp,
                     "pangrova.protocol.v2.NoMoreExchangeSourcesRequest")) {
    return;
  }
  std::shared_ptr<pangrova::executor::TaskExecutor> executor;
  try {
    executor = HandleRegistry::instance().acquire(handle);
  } catch (const std::runtime_error& error) {
    resp = errorResponse(error.what());
    return;
  }
  if (!validateAttemptIdentity(cmd, *executor, resp)) {
    return;
  }
  pangrova::velox::PendingEndpointRegistry::instance().sealInput(
      handle, request.plan_node_id(), request.exchange_id(),
      request.expected_source_partitions());
  resp = successResponse();
}

void handleConnectExchangeSource(const proto::RuntimeCommand& cmd,
                                 int64_t executor_handle,
                                 proto::RuntimeCommandResponse& resp) {
  if (executor_handle == 0) {
    resp = errorResponse(
        "CONNECT_EXCHANGE_SOURCE requires a non-zero executor_handle");
    return;
  }
  std::shared_ptr<pangrova::executor::TaskExecutor> executor;
  try {
    executor = HandleRegistry::instance().acquire(executor_handle);
  } catch (const std::runtime_error& error) {
    resp = errorResponse(error.what());
    return;
  }
  proto::ConnectExchangeSourceRequest request;
  if (!unpackPayload(cmd, &request, resp,
                     "pangrova.protocol.v2.ConnectExchangeSourceRequest")) {
    return;
  }
  if (!validateAttemptIdentity(cmd, *executor, resp)) {
    return;
  }
  if (request.plan_node_id().empty() || request.exchange_id().empty()) {
    resp = errorResponse(
        "Exchange source requires plan node and exchange identities",
        proto::ERROR_CLASS_INVALID_REQUEST, "INVALID_EXCHANGE_SOURCE_IDENTITY");
    return;
  }
  if (request.transport_case() ==
      proto::ConnectExchangeSourceRequest::kExternal) {
    const auto validation =
        validateExternalSource(request.external(), *executor);
    if (!validation.empty()) {
      resp = errorResponse(validation, proto::ERROR_CLASS_INVALID_REQUEST,
                           "INVALID_EXTERNAL_SHUFFLE_SOURCE");
      return;
    }
    pangrova::velox::PendingEndpointRegistry::instance().registerExternalSource(
        executor_handle, request.plan_node_id(), request.exchange_id(),
        request.external().deadline_epoch_ms(),
        externalShuffleMaxBatchBytes(*executor));
    PANGROVA_LOG(INFO) << "event=external_exchange_source_registered"
                       << " plan_node_id=" << request.plan_node_id()
                       << " exchange_id=" << request.exchange_id()
                       << " shuffle_id=" << request.external().shuffle_id()
                       << " output_partition_id="
                       << request.external().output_partition_id();
    resp = successResponse();
    return;
  }
  if (request.transport_case() !=
      proto::ConnectExchangeSourceRequest::kDirect) {
    resp = errorResponse(
        "Exchange source requires exactly one direct or external transport",
        proto::ERROR_CLASS_INVALID_REQUEST, "MISSING_EXCHANGE_TRANSPORT");
    return;
  }
  const auto& source = request.direct();
  if (source.plan_epoch() != executor->planEpoch() ||
      source.output_partition_id() != executor->partitionId()) {
    resp = errorResponse(
        "Direct exchange source identity does not match the executor",
        proto::ERROR_CLASS_POLICY_DENIED,
        "DIRECT_EXCHANGE_SOURCE_FENCE_MISMATCH");
    return;
  }
  pangrova::velox::PendingEndpoint endpoint;
  endpoint.source_task_id = source.source_task_id();
  endpoint.source_stage_id = source.source_stage_id();
  endpoint.source_partition_id = source.source_partition_id();
  endpoint.output_partition_id = source.output_partition_id();
  endpoint.plan_epoch = source.plan_epoch();
  endpoint.attempt_id = source.attempt_id();
  endpoint.fencing_token = source.fencing_token();
  endpoint.host = source.host();
  endpoint.port = static_cast<int>(source.port());
  endpoint.tls_identity = source.tls_identity();
  endpoint.signed_ticket = source.signed_ticket();
  endpoint.ticket_expires_at_epoch_ms = source.ticket_expires_at_epoch_ms();
  endpoint.expected_rows = source.expected_rows();
  endpoint.expected_bytes = source.expected_bytes();

  pangrova::velox::PendingEndpointRegistry::instance().registerSource(
      executor_handle, request.plan_node_id(), request.exchange_id(),
      std::move(endpoint));

  PANGROVA_LOG(INFO) << "event=exchange_source_endpoint_registered"
                     << " plan_node_id=" << request.plan_node_id()
                     << " exchange_id=" << request.exchange_id()
                     << " source_task_id=" << source.source_task_id()
                     << " host=" << source.host() << " port=" << source.port()
                     << " source_stage_id=" << source.source_stage_id()
                     << " source_partition_id=" << source.source_partition_id()
                     << " output_partition_id=" << source.output_partition_id();

  resp = successResponse();
}

void handlePushExchangeBatch(const proto::RuntimeCommand& cmd,
                             int64_t executor_handle,
                             proto::RuntimeCommandResponse& resp) {
  if (executor_handle <= 0) {
    resp = errorResponse(
        "PUSH_EXCHANGE_BATCH requires a positive executor handle",
        proto::ERROR_CLASS_INVALID_REQUEST, "MISSING_EXECUTOR_HANDLE");
    return;
  }
  proto::PushExchangeBatchRequest request;
  if (!unpackPayload(cmd, &request, resp,
                     "pangrova.protocol.v2.PushExchangeBatchRequest")) {
    return;
  }
  auto executor = HandleRegistry::instance().acquire(executor_handle);
  if (!validateAttemptIdentity(cmd, *executor, resp)) {
    return;
  }
  if (request.plan_node_id().empty() || request.exchange_id().empty() ||
      !request.has_chunk()) {
    resp = errorResponse(
        "External exchange batch requires identities and one canonical chunk",
        proto::ERROR_CLASS_INVALID_REQUEST, "INVALID_EXCHANGE_BATCH");
    return;
  }
  const auto& chunk = request.chunk();
  const auto max_batch_bytes = externalShuffleMaxBatchBytes(*executor);
  const bool valid_bounds =
      chunk.batch_sequence() >= 0 && chunk.chunk_count() > 0 &&
      chunk.chunk_index() < chunk.chunk_count() && !chunk.data().empty() &&
      chunk.data().size() <= pangrova::velox::kExternalShuffleChunkBytes &&
      chunk.batch_size_bytes() > 0 &&
      chunk.batch_size_bytes() <= max_batch_bytes;
  const bool valid_checksums =
      chunk.has_chunk_checksum() && chunk.has_batch_checksum() &&
      chunk.chunk_checksum().algorithm() ==
          proto::Checksum::ALGORITHM_SHA256 &&
      chunk.batch_checksum().algorithm() ==
          proto::Checksum::ALGORITHM_SHA256 &&
      chunk.chunk_checksum().digest().size() == 32 &&
      chunk.batch_checksum().digest().size() == 32;
  if (!valid_bounds || !valid_checksums) {
    resp = errorResponse(
        "External exchange chunk violates canonical bounds or checksums",
        proto::ERROR_CLASS_INVALID_REQUEST, "INVALID_EXCHANGE_CHUNK");
    return;
  }
  const uint64_t expected_chunks =
      1 + (chunk.batch_size_bytes() - 1) /
          pangrova::velox::kExternalShuffleChunkBytes;
  if (expected_chunks != chunk.chunk_count()) {
    resp = errorResponse(
        "External exchange chunk count is not canonical",
        proto::ERROR_CLASS_INVALID_REQUEST,
        "INVALID_EXCHANGE_CHUNK_LAYOUT");
    return;
  }
  const uint64_t expected_chunk_size =
      chunk.chunk_index() + 1 < chunk.chunk_count()
      ? pangrova::velox::kExternalShuffleChunkBytes
      : chunk.batch_size_bytes() -
          static_cast<uint64_t>(chunk.chunk_index()) *
              pangrova::velox::kExternalShuffleChunkBytes;
  if (expected_chunk_size != chunk.data().size()) {
    resp = errorResponse(
        "External exchange chunk layout is not canonical",
        proto::ERROR_CLASS_INVALID_REQUEST,
        "INVALID_EXCHANGE_CHUNK_LAYOUT");
    return;
  }
  const auto actual_checksum = sha256(chunk.data());
  if (!constantTimeEqual(
          chunk.chunk_checksum().digest(), actual_checksum)) {
    resp = errorResponse("External exchange chunk checksum mismatch",
                         proto::ERROR_CLASS_DATA_CORRUPTION,
                         "EXCHANGE_CHUNK_CHECKSUM_MISMATCH");
    return;
  }
  pangrova::velox::ExternalShuffleChunk native_chunk;
  native_chunk.batch_sequence = chunk.batch_sequence();
  native_chunk.chunk_index = chunk.chunk_index();
  native_chunk.chunk_count = chunk.chunk_count();
  native_chunk.batch_size_bytes = chunk.batch_size_bytes();
  native_chunk.data = std::move(*request.mutable_chunk()->mutable_data());
  native_chunk.batch_checksum = chunk.batch_checksum().digest();
  try {
    pangrova::velox::PendingEndpointRegistry::instance().pushExternalBatch(
        executor_handle, request.plan_node_id(), request.exchange_id(),
        std::move(native_chunk));
  } catch (const pangrova::velox::ExternalShuffleChecksumError& error) {
    resp = errorResponse(error.what(), proto::ERROR_CLASS_DATA_CORRUPTION,
                         "EXCHANGE_BATCH_CHECKSUM_MISMATCH");
    return;
  } catch (const std::invalid_argument& error) {
    resp = errorResponse(error.what(), proto::ERROR_CLASS_INVALID_REQUEST,
                         "INVALID_EXCHANGE_CHUNK");
    return;
  } catch (const std::logic_error& error) {
    resp = errorResponse(error.what(), proto::ERROR_CLASS_INVALID_REQUEST,
                         "INVALID_EXCHANGE_CHUNK_STATE");
    return;
  } catch (const std::runtime_error& error) {
    resp = errorResponse(error.what(), proto::ERROR_CLASS_TRANSPORT,
                         "EXCHANGE_INGRESS_UNAVAILABLE");
    return;
  }
  resp = successResponse();
}

void handleFinishExchangeSource(const proto::RuntimeCommand& cmd,
                                int64_t executor_handle,
                                proto::RuntimeCommandResponse& resp) {
  if (executor_handle <= 0) {
    resp = errorResponse(
        "FINISH_EXCHANGE_SOURCE requires a positive executor handle",
        proto::ERROR_CLASS_INVALID_REQUEST, "MISSING_EXECUTOR_HANDLE");
    return;
  }
  proto::FinishExchangeSourceRequest request;
  if (!unpackPayload(cmd, &request, resp,
                     "pangrova.protocol.v2.FinishExchangeSourceRequest")) {
    return;
  }
  auto executor = HandleRegistry::instance().acquire(executor_handle);
  if (!validateAttemptIdentity(cmd, *executor, resp)) {
    return;
  }
  if (request.plan_node_id().empty() || request.exchange_id().empty()) {
    resp = errorResponse(
        "External exchange terminal requires plan node and exchange identities",
        proto::ERROR_CLASS_INVALID_REQUEST, "INVALID_EXCHANGE_TERMINAL");
    return;
  }
  std::string failure;
  if (request.has_failure()) {
    if (request.failure().error_class() == proto::ERROR_CLASS_UNSPECIFIED ||
        request.failure().code().empty() ||
        request.failure().message().empty() ||
        request.failure().failed_component().empty()) {
      resp = errorResponse("External exchange failure must contain class, "
                           "code, component, and message",
                           proto::ERROR_CLASS_INVALID_REQUEST,
                           "INVALID_EXCHANGE_FAILURE");
      return;
    }
    failure = formatExternalFailure(request.failure());
  }
  pangrova::velox::PendingEndpointRegistry::instance().finishExternalSource(
      executor_handle, request.plan_node_id(), request.exchange_id(),
      request.last_sequence(), failure);
  resp = successResponse();
}

void handleCancelAttempt(int64_t executor_handle,
                         proto::RuntimeCommandResponse& response) {
  if (executor_handle <= 0) {
    response =
        errorResponse("CANCEL_ATTEMPT requires a positive executor handle");
    return;
  }
  try {
    HandleRegistry::instance().acquire(executor_handle)->cancel();
    response = successResponse();
  } catch (const std::runtime_error& error) {
    response = errorResponse(error.what());
  }
}

std::vector<uint8_t> dispatchParsedCommand(const proto::RuntimeCommand& cmd,
                                           int64_t executor_handle) {
  proto::RuntimeCommandResponse resp;
  if (!cmd.has_protocol() || cmd.protocol().major() != 2) {
    return serializeResponse(errorResponse(
        "RuntimeCommand requires canonical protocol major 2",
        proto::ERROR_CLASS_PROTOCOL_MISMATCH, "PROTOCOL_MAJOR_MISMATCH"));
  }
  try {
    switch (cmd.command_type()) {
    case proto::RUNTIME_COMMAND_TYPE_GET_RESOURCE_DRAIN_EVIDENCE:
      resp = resourceDrainEvidence(cmd, executor_handle);
      break;
    case proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT:
      resp = NativeDatasetControl::schemaFingerprint(cmd);
      break;
    case proto::RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE:
      resp = NativeDatasetControl::finalizeDistributedWrite(cmd);
      break;
    case proto::RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS:
      resp = NativeDatasetControl::scanPartitions(cmd);
      break;
    case proto::RUNTIME_COMMAND_TYPE_INJECT_RUNTIME_FILTER:
      handleInjectRuntimeFilter(cmd, executor_handle, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_UPDATE_RESOURCE_GRANT:
      handleUpdateResourceGrant(cmd, executor_handle, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_PREFETCH_ARTIFACTS:
      handlePrefetchArtifacts(cmd, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_EVICT_CACHE:
      handleEvictCache(cmd, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_GET_METRICS:
      handleGetMetrics(cmd, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_GET_CAPABILITIES:
      handleGetCapabilities(cmd, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_MEMORY_PRESSURE:
      handleMemoryPressure(cmd, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_START_RESULT_SERVER:
      handleStartResultServer(cmd, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_STOP_RESULT_SERVER:
      handleStopResultServer(cmd, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_START_SHUFFLE_SERVER:
      handleStartShuffleServer(cmd, executor_handle, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE:
      handleConnectExchangeSource(cmd, executor_handle, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_STOP_SHUFFLE_SERVER:
      handleStopShuffleServer(cmd, executor_handle, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_NO_MORE_EXCHANGE_SOURCES:
      handleNoMoreExchangeSources(cmd, executor_handle, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH:
      handlePushExchangeBatch(cmd, executor_handle, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE:
      handleFinishExchangeSource(cmd, executor_handle, resp);
      break;
    case proto::RUNTIME_COMMAND_TYPE_CANCEL_ATTEMPT:
      handleCancelAttempt(executor_handle, resp);
      break;
    default:
      PANGROVA_LOG(ERROR) << "event=unknown_command_type"
                          << " command_type="
                          << static_cast<int>(cmd.command_type());
      resp = errorResponse(
          "Unknown command type: " +
              std::to_string(static_cast<int>(cmd.command_type())),
          proto::ERROR_CLASS_INVALID_REQUEST, "UNKNOWN_COMMAND_TYPE");
      break;
    }
  } catch (const std::exception& e) {
    resp = errorResponse(std::string("Command handler failed: ") + e.what());
  } catch (...) {
    resp = errorResponse("Command handler failed: unknown exception");
  }

  return serializeResponse(resp);
}

} // anonymous namespace

std::vector<uint8_t>
dispatchCommand(const std::vector<uint8_t>& command_proto) {
  proto::RuntimeCommand cmd;
  if (!cmd.ParseFromArray(command_proto.data(),
                          static_cast<int>(command_proto.size()))) {
    return serializeResponse(errorResponse(
        "Failed to parse RuntimeCommand proto",
        proto::ERROR_CLASS_INVALID_REQUEST, "MALFORMED_COMMAND_ENVELOPE"));
  }

  return dispatchParsedCommand(cmd, 0);
}

std::vector<uint8_t>
dispatchCommandWithHandle(const std::vector<uint8_t>& command_proto,
                          int64_t executor_handle) {
  proto::RuntimeCommand cmd;
  if (!cmd.ParseFromArray(command_proto.data(),
                          static_cast<int>(command_proto.size()))) {
    return serializeResponse(errorResponse(
        "Failed to parse RuntimeCommand proto",
        proto::ERROR_CLASS_INVALID_REQUEST, "MALFORMED_COMMAND_ENVELOPE"));
  }
  return dispatchParsedCommand(cmd, executor_handle);
}

void shutdownFlightResultServer() {
#if PANGROVA_HAS_FLIGHT
  FlightServerRegistry::instance().shutdownAll();
#endif
}

void cleanupShuffleServerForHandle(int64_t executor_handle) {
#if PANGROVA_HAS_FLIGHT
  FlightServerRegistry::instance().unregisterShuffleHandle(executor_handle);

  PANGROVA_LOG(INFO) << "event=shuffle_handle_cleaned_on_release"
                     << " executor_handle=" << executor_handle;
#else
  (void)executor_handle;
#endif
}

} // namespace pangrova::jni
