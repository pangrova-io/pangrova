// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Command dispatcher: routes NativeCommand proto to per-type handlers.
// Each handler is a standalone function — single responsibility per command.

#include "jni/command/NativeCommandDispatcher.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <memory>
#include <string>

#include "split/PathUtils.h"
#include "jni/bridge/JniBridge.h"
#include "jni/command/FlightServerRegistry.h"
#include "jni/command/StatusSerializer.h"
#include "jni/registry/HandleRegistry.h"
#include "jni/RuntimeInitializer.h"
#include "memory/MemoryPressure.h"
#include "observability/PangrovaLog.h"
#if PANGROVA_HAS_FLIGHT
#include "result/FlightResultServer.h"
#include "shuffle/FlightShuffleServer.h"
#endif
#include "velox/exchange/PendingEndpointRegistry.h"
#include "velox/memory/VeloxMemoryGovernor.h"

// Velox AsyncDataCache for cache operations.
#include "compat/NativeFeatureDetection.h"

#if __has_include("pangrova/native/protocol/v1/command.pb.h")
#include "pangrova/native/protocol/v1/task.pb.h"
#include "pangrova/native/protocol/v1/split.pb.h"
#include "pangrova/native/protocol/v1/command.pb.h"
#include "pangrova/native/protocol/v1/dynamic_filter.pb.h"
#include "pangrova/native/protocol/v1/memory.pb.h"
#include "pangrova/native/protocol/v1/cache.pb.h"
#define PANGROVA_HAS_PROTO 1
#else
#define PANGROVA_HAS_PROTO 0
#endif

#if PANGROVA_HAS_PROTO

namespace pangrova::jni {
namespace {

namespace proto = pangrova::native::protocol::v1;

// Serialize a NativeCommandResponse to bytes.
std::vector<uint8_t> serializeResponse(
    const proto::NativeCommandResponse& resp) {
  std::vector<uint8_t> out(resp.ByteSizeLong());
  resp.SerializeToArray(out.data(), static_cast<int>(out.size()));
  return out;
}

// Build an error response with the given message.
proto::NativeCommandResponse errorResponse(const std::string& message) {
  proto::NativeCommandResponse resp;
  resp.set_success(false);
  resp.set_error_message(message);
  return resp;
}

// Build a success response (no result payload).
proto::NativeCommandResponse successResponse() {
  proto::NativeCommandResponse resp;
  resp.set_success(true);
  return resp;
}

// --- Per-command handlers ---

void handleInjectDynamicFilter(const proto::NativeCommand& cmd,
                               proto::NativeCommandResponse& resp) {
  proto::InjectDynamicFilterPayload payload;
  if (!payload.ParseFromString(cmd.payload())) {
    resp = errorResponse("Failed to parse InjectDynamicFilterPayload");
    return;
  }

  std::shared_ptr<pangrova::executor::TaskExecutor> executor;
  try {
    executor = HandleRegistry::instance().acquire(cmd.executor_handle());
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

void handleUpdateMemoryLimit(const proto::NativeCommand& cmd,
                             proto::NativeCommandResponse& resp) {
  proto::UpdateMemoryLimitPayload payload;
  if (!payload.ParseFromString(cmd.payload())) {
    resp = errorResponse("Failed to parse UpdateMemoryLimitPayload");
    return;
  }

  std::shared_ptr<pangrova::executor::TaskExecutor> executor;
  try {
    executor = HandleRegistry::instance().acquire(cmd.executor_handle());
  } catch (const std::runtime_error& e) {
    resp = errorResponse(e.what());
    return;
  }

  executor->updateMemoryLimit(payload.new_limit_bytes());
  resp = successResponse();
}

void handlePrefetchSplits(const proto::NativeCommand& cmd,
                          proto::NativeCommandResponse& resp) {
  // Parse PrefetchSplitsRequest to extract file paths for cache warming.
  // On parse failure, silently succeed — prefetch is best-effort.
#if PANGROVA_HAS_VELOX_ASYNC_CACHE && PANGROVA_HAS_VELOX_FILE && PANGROVA_HAS_FOLLY_EXECUTOR
  proto::PrefetchSplitsRequest payload;
  if (!payload.ParseFromString(cmd.payload())) {
    resp = successResponse();
    return;
  }

  auto* cache = globalAsyncDataCache();
  if (!cache) {
    // Cache disabled — silently ignore prefetch requests.
    resp = successResponse();
    return;
  }

  auto executor = globalExecutor();
  if (!executor) {
    resp = successResponse();
    return;
  }

  // Submit async prefetch tasks for each split file.
  // Each task reads the file footer region to warm the OS page cache
  // and Velox AsyncDataCache. The handler returns immediately.
  for (const auto& split : payload.splits()) {
    auto filePath = split.file_path();
    if (filePath.empty()) {
      continue;
    }

    // Normalize file:// prefix for local file access.
    filePath = pangrova::split::stripFilePrefix(filePath);

    // Async non-blocking prefetch: read footer region to warm cache.
    executor->add([filePath]() {
      try {
        auto localFile =
            std::make_shared<facebook::velox::LocalReadFile>(filePath);
        auto fileSize = localFile->size();
        if (fileSize == 0) {
          return;
        }

        // Read the last min(64KB, fileSize) bytes — covers the Parquet
        // footer and column statistics for most files.
        constexpr int64_t kPrefetchFooterBytes = 64 * 1024;
        auto readSize = std::min(
            static_cast<int64_t>(fileSize), kPrefetchFooterBytes);
        auto offset = static_cast<int64_t>(fileSize) - readSize;

        std::string buffer(readSize, '\0');
        localFile->pread(offset, readSize, buffer.data());
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

void handleEvictCache(const proto::NativeCommand& cmd,
                      proto::NativeCommandResponse& resp) {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
  auto* cache = globalAsyncDataCache();
  if (!cache) {
    resp = successResponse();
    return;
  }

  proto::EvictCachePayload payload;
  if (!payload.ParseFromString(cmd.payload())) {
    resp = errorResponse("Failed to parse EvictCachePayload");
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
    resp = errorResponse(
        "Pattern-based cache eviction is not supported. "
        "Use pattern='*' to explicitly clear all cache entries. "
        "Received pattern='" + payload.pattern() + "'");
    return;
  }

  std::cerr << "level=INFO event=cache_evict_full_clear"
            << " pattern=" << payload.pattern()
            << std::endl;

  // Velox AsyncDataCache clear() evicts all entries.
  cache->clear();
  resp = successResponse();
#else
  (void)cmd;
  resp = successResponse();
#endif
}

void handleGetMetrics(const proto::NativeCommand& /*cmd*/,
                      proto::NativeCommandResponse& resp) {
  // Collect global memory metrics from VeloxMemoryGovernor.
  proto::TaskStats aggregated;

  auto* bridge = globalVeloxMemoryGovernor();
  if (bridge) {
    auto snapshot = bridge->getMemorySnapshot();
    aggregated.set_memory_bytes(snapshot.current_used_bytes);
    // P1-BUG-9: Use actual peak from snapshot instead of totalMemory().
    aggregated.set_peak_memory_bytes(snapshot.peak_used_bytes);
  }

  std::vector<uint8_t> payload(aggregated.ByteSizeLong());
  aggregated.SerializeToArray(payload.data(),
                              static_cast<int>(payload.size()));

  resp = successResponse();
  resp.set_result_payload(payload.data(), payload.size());
}

void handleGetCacheStats(const proto::NativeCommand& /*cmd*/,
                         proto::NativeCommandResponse& resp) {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
  auto* cache = globalAsyncDataCache();
  if (!cache) {
    resp = successResponse();
    return;
  }

  auto veloxStats = cache->refreshStats();
  auto statsBytes = serializeNativeCacheStats(veloxStats);
  resp = successResponse();
  resp.set_result_payload(statsBytes.data(), statsBytes.size());
#else
  resp = successResponse();
#endif
}

void handleMemoryPressure(const proto::NativeCommand& cmd,
                          proto::NativeCommandResponse& resp) {
  proto::MemoryPressurePayload payload;
  if (!payload.ParseFromString(cmd.payload())) {
    resp = errorResponse("Failed to parse MemoryPressurePayload");
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
    case proto::MEMORY_PRESSURE_HIGH:
      usage_percent = pangrova::memory::kPressureThresholdHigh + 1.0;
      break;
    case proto::MEMORY_PRESSURE_CRITICAL:
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

void handleGetCapabilities(const proto::NativeCommand& /*cmd*/,
                           proto::NativeCommandResponse& resp) {
  // Build NativeCapabilityResponse with the Rel/Expr/Function types
  // supported by VeloxPlanConverter. These lists must stay aligned with
  // VeloxPlanConverter::convertRel() switch cases and convertExpression().
  proto::NativeCapabilityResponse capabilities;

  // Supported Rel types (aligned with VeloxPlanConverter::convertRel).
  capabilities.add_supported_rel_types("ReadRel");
  capabilities.add_supported_rel_types("FilterRel");
  capabilities.add_supported_rel_types("ProjectRel");
  capabilities.add_supported_rel_types("AggregateRel");
  capabilities.add_supported_rel_types("HashJoinRel");
  capabilities.add_supported_rel_types("JoinRel");
  capabilities.add_supported_rel_types("ExchangeRel");
  capabilities.add_supported_rel_types("SortRel");
  capabilities.add_supported_rel_types("FetchRel");
  capabilities.add_supported_rel_types("ConsistentPartitionWindowRel");
  capabilities.add_supported_rel_types("SetRel");
  capabilities.add_supported_rel_types("ExtensionMultiRel");

  // Supported Expression types (aligned with VeloxPlanConverter::convertExpression).
  capabilities.add_supported_expr_types("FieldReference");
  capabilities.add_supported_expr_types("Literal");
  capabilities.add_supported_expr_types("ScalarFunction");
  capabilities.add_supported_expr_types("Cast");
  capabilities.add_supported_expr_types("IfThen");
  capabilities.add_supported_expr_types("WindowFunction");

  // Supported functions: populated from the function_map_ built during plan
  // conversion. At capabilities-query time we report the empty set because
  // function support is plan-dependent (resolved via extension declarations).
  // Java side uses defaults() for function validation.

  capabilities.set_velox_version("0.1.0");

  std::vector<uint8_t> payload(capabilities.ByteSizeLong());
  capabilities.SerializeToArray(payload.data(),
                                static_cast<int>(payload.size()));

  resp = successResponse();
  resp.set_result_payload(payload.data(), payload.size());
}

void handleStartResultServer(const proto::NativeCommand& cmd,
                              proto::NativeCommandResponse& resp) {
#if PANGROVA_HAS_FLIGHT
  proto::StartResultServerPayload payload;
  if (!payload.ParseFromString(cmd.payload())) {
    resp = errorResponse("Failed to parse StartResultServerPayload");
    return;
  }

  auto& registry = FlightServerRegistry::instance();
  auto* existing = registry.resultServer();
  if (existing && existing->isRunning()) {
    // Already running — return the existing bound port.
    proto::StartResultServerResponsePayload result;
    result.set_bound_port(existing->boundPort());
    std::vector<uint8_t> result_bytes(result.ByteSizeLong());
    result.SerializeToArray(result_bytes.data(),
                            static_cast<int>(result_bytes.size()));
    resp = successResponse();
    resp.set_result_payload(result_bytes.data(), result_bytes.size());
    return;
  }

  pangrova::result::FlightResultServerConfig config;
  config.port = payload.preferred_port();

  auto server =
      std::make_unique<pangrova::result::FlightResultServer>(config);
  int bound_port = server->start();
  registry.setResultServer(std::move(server));

  proto::StartResultServerResponsePayload result;
  result.set_bound_port(bound_port);
  std::vector<uint8_t> result_bytes(result.ByteSizeLong());
  result.SerializeToArray(result_bytes.data(),
                          static_cast<int>(result_bytes.size()));

  resp = successResponse();
  resp.set_result_payload(result_bytes.data(), result_bytes.size());
#else
  (void)cmd;
  resp = errorResponse("Arrow Flight not available — built without PANGROVA_HAS_FLIGHT");
#endif
}

void handleStopResultServer(const proto::NativeCommand& /*cmd*/,
                             proto::NativeCommandResponse& resp) {
#if PANGROVA_HAS_FLIGHT
  FlightServerRegistry::instance().shutdownResultServer();
  resp = successResponse();
#else
  resp = errorResponse("Arrow Flight not available — built without PANGROVA_HAS_FLIGHT");
#endif
}

void handleStartFlightServer(const proto::NativeCommand& cmd,
                              proto::NativeCommandResponse& resp) {
#if PANGROVA_HAS_FLIGHT
  auto handle = cmd.executor_handle();
  if (handle == 0) {
    resp = errorResponse("START_FLIGHT_SERVER requires a non-zero executor_handle");
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

  // Register this handle with the shared multiplexed shuffle server.
  // The server is auto-started on first registration and all handles
  // share a single port, eliminating per-handle port exhaustion.
  auto& registry = FlightServerRegistry::instance();
  registry.registerShuffleHandle(handle, queue);
  int bound_port = registry.ensureShuffleServerRunning();

  proto::StartResultServerResponsePayload result;
  result.set_bound_port(bound_port);
  std::vector<uint8_t> result_bytes(result.ByteSizeLong());
  result.SerializeToArray(result_bytes.data(),
                          static_cast<int>(result_bytes.size()));
  resp = successResponse();
  resp.set_result_payload(result_bytes.data(), result_bytes.size());
#else
  (void)cmd;
  resp = errorResponse("Arrow Flight not available — built without PANGROVA_HAS_FLIGHT");
#endif
}

// Unregisters the executor handle from the shared multiplexed shuffle server.
// Called from Java when a shuffle task completes or fails, releasing the
// handle's demuxer resources while the shared server continues serving
// other handles.
void handleStopShuffleServer(const proto::NativeCommand& cmd,
                              proto::NativeCommandResponse& resp) {
#if PANGROVA_HAS_FLIGHT
  auto handle = cmd.executor_handle();
  if (handle == 0) {
    resp = errorResponse("STOP_SHUFFLE_SERVER requires a non-zero executor_handle");
    return;
  }

  FlightServerRegistry::instance().unregisterShuffleHandle(handle);

  PANGROVA_LOG(INFO) << "event=shuffle_handle_unregistered"
                    << " executor_handle=" << handle;
  resp = successResponse();
#else
  (void)cmd;
  resp = errorResponse("Arrow Flight not available — built without PANGROVA_HAS_FLIGHT");
#endif
}

// Signals the native engine that no more exchange sources will be added
// for the given executor. Velox ExchangeNode uses this to know when all
// upstream data has been accounted for, preventing indefinite waits when
// the source count is zero or all sources have been registered.
void handleNoMoreExchangeSources(const proto::NativeCommand& cmd,
                                  proto::NativeCommandResponse& resp) {
  auto handle = cmd.executor_handle();
  if (handle == 0) {
    resp = errorResponse("NO_MORE_EXCHANGE_SOURCES requires a non-zero executor_handle");
    return;
  }

  std::shared_ptr<pangrova::executor::TaskExecutor> executor;
  try {
    executor = HandleRegistry::instance().acquire(handle);
  } catch (const std::runtime_error& e) {
    resp = errorResponse(e.what());
    return;
  }

  executor->noMoreExchangeSources();
  resp = successResponse();
}

void handleConnectExchangeSource(const proto::NativeCommand& cmd,
                                  proto::NativeCommandResponse& resp) {
  proto::ConnectExchangeSourcePayload payload;
  if (!payload.ParseFromString(cmd.payload())) {
    resp = errorResponse("Failed to parse ConnectExchangeSourcePayload");
    return;
  }

  if (payload.source_id().empty()) {
    resp = errorResponse("CONNECT_EXCHANGE_SOURCE: source_id must not be empty");
    return;
  }
  if (payload.host().empty() || payload.port() <= 0) {
    resp = errorResponse(
        "CONNECT_EXCHANGE_SOURCE: host and port must be specified. "
        "source_id=" + payload.source_id());
    return;
  }

  pangrova::velox::PendingEndpoint endpoint;
  endpoint.host = payload.host();
  endpoint.port = payload.port();
  endpoint.executor_handle = payload.executor_handle();
  endpoint.query_id = payload.query_id();
  endpoint.stage_id = payload.stage_id();
  endpoint.partition_id = payload.partition_id();

  pangrova::velox::PendingEndpointRegistry::instance().put(
      payload.source_id(), std::move(endpoint));

  PANGROVA_LOG(INFO) << "event=exchange_source_endpoint_registered"
                   << " source_id=" << payload.source_id()
                   << " host=" << payload.host()
                   << " port=" << payload.port()
                   << " query_id=" << payload.query_id()
                   << " stage_id=" << payload.stage_id()
                   << " partition_id=" << payload.partition_id();

  resp = successResponse();
}

void handleStartNativeExchange(const proto::NativeCommand& cmd,
                                proto::NativeCommandResponse& resp) {
  proto::StartNativeExchangePayload payload;
  if (!payload.ParseFromString(cmd.payload())) {
    resp = errorResponse("Failed to parse StartNativeExchangePayload");
    return;
  }

  for (const auto& source : payload.sources()) {
    if (source.source_task_id().empty()) {
      continue;
    }
    pangrova::velox::PendingEndpoint endpoint;
    endpoint.host = source.host();
    endpoint.port = source.port();
    endpoint.partition_id = source.partition_id();

    pangrova::velox::PendingEndpointRegistry::instance().put(
        source.source_task_id(), std::move(endpoint));
  }

  PANGROVA_LOG(INFO) << "event=native_exchange_started"
                   << " source_count=" << payload.sources_size();

  resp = successResponse();
}

// Internal dispatch on a parsed proto object, avoiding
// redundant serialize-deserialize round-trips.
std::vector<uint8_t> dispatchParsedCommand(proto::NativeCommand& cmd) {
  proto::NativeCommandResponse resp;
  try {
    switch (cmd.command_type()) {
      case proto::NATIVE_COMMAND_INJECT_DYNAMIC_FILTER:
        handleInjectDynamicFilter(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_UPDATE_MEMORY_LIMIT:
        handleUpdateMemoryLimit(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_PREFETCH_SPLITS:
        handlePrefetchSplits(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_EVICT_CACHE:
        handleEvictCache(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_GET_METRICS:
        handleGetMetrics(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_GET_CACHE_STATS:
        handleGetCacheStats(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_GET_CAPABILITIES:
        handleGetCapabilities(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_MEMORY_PRESSURE:
        handleMemoryPressure(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_START_RESULT_SERVER:
        handleStartResultServer(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_STOP_RESULT_SERVER:
        handleStopResultServer(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_START_FLIGHT_SERVER:
        handleStartFlightServer(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_START_NATIVE_EXCHANGE:
        handleStartNativeExchange(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_CONNECT_EXCHANGE_SOURCE:
        handleConnectExchangeSource(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_STOP_SHUFFLE_SERVER:
        handleStopShuffleServer(cmd, resp);
        break;
      case proto::NATIVE_COMMAND_NO_MORE_EXCHANGE_SOURCES:
        handleNoMoreExchangeSources(cmd, resp);
        break;
      default:
        PANGROVA_LOG(ERROR) << "event=unknown_command_type"
                          << " command_type=" << static_cast<int>(cmd.command_type());
        resp = errorResponse(
            "Unknown command type: " +
            std::to_string(static_cast<int>(cmd.command_type())));
        break;
    }
  } catch (const std::exception& e) {
    resp = errorResponse(
        std::string("Command handler failed: ") + e.what());
  } catch (...) {
    resp = errorResponse("Command handler failed: unknown exception");
  }

  return serializeResponse(resp);
}

}  // anonymous namespace

std::vector<uint8_t> dispatchCommand(
    const std::vector<uint8_t>& command_proto) {
  proto::NativeCommand cmd;
  if (!cmd.ParseFromArray(command_proto.data(),
                          static_cast<int>(command_proto.size()))) {
    return serializeResponse(
        errorResponse("Failed to parse NativeCommand proto"));
  }

  return dispatchParsedCommand(cmd);
}

std::vector<uint8_t> dispatchCommandWithTimeout(
    const std::vector<uint8_t>& command_proto,
    int64_t timeout_ms) {
  if (timeout_ms <= 0) {
    return dispatchCommand(command_proto);
  }

  auto future = std::async(std::launch::async, [&]() {
    return dispatchCommand(command_proto);
  });

  auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
  if (status == std::future_status::timeout) {
    return serializeResponse(errorResponse(
        "Command timed out after " + std::to_string(timeout_ms) + "ms"));
  }
  return future.get();
}

std::vector<uint8_t> dispatchCommandWithHandle(
    const std::vector<uint8_t>& command_proto,
    int64_t executor_handle) {
  proto::NativeCommand cmd;
  if (!cmd.ParseFromArray(command_proto.data(),
                          static_cast<int>(command_proto.size()))) {
    return serializeResponse(
        errorResponse("Failed to parse NativeCommand proto"));
  }

  // P0-BUG-4: Override proto executor_handle with the JNI-authoritative value.
  // This ensures the JNI parameter is the single source of truth for routing.
  cmd.set_executor_handle(executor_handle);

  // P2-BUG-10: Dispatch directly on the parsed+patched proto object,
  // eliminating the redundant serialize→deserialize round-trip.
  return dispatchParsedCommand(cmd);
}

std::vector<uint8_t> dispatchCommandWithHandleAndTimeout(
    const std::vector<uint8_t>& command_proto,
    int64_t executor_handle,
    int64_t timeout_ms) {
  if (timeout_ms <= 0) {
    return dispatchCommandWithHandle(command_proto, executor_handle);
  }

  auto future = std::async(std::launch::async, [&]() {
    return dispatchCommandWithHandle(command_proto, executor_handle);
  });

  auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
  if (status == std::future_status::timeout) {
    return serializeResponse(errorResponse(
        "Command timed out after " + std::to_string(timeout_ms) + "ms"));
  }
  return future.get();
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

}  // namespace pangrova::jni

#else  // !PANGROVA_HAS_PROTO

namespace pangrova::jni {

std::vector<uint8_t> dispatchCommand(
    const std::vector<uint8_t>& /*command_proto*/) {
  // Proto support not compiled — return a minimal error indicator.
  // Byte 0x00 signals failure to Java callers that check extractResultPayload().
  // This is preferable to an empty vector which is ambiguous (success vs error).
  static const std::vector<uint8_t> kNoProtoError = {0x00};
  return kNoProtoError;
}

std::vector<uint8_t> dispatchCommandWithTimeout(
    const std::vector<uint8_t>& /*command_proto*/,
    int64_t /*timeout_ms*/) {
  static const std::vector<uint8_t> kNoProtoError = {0x00};
  return kNoProtoError;
}

std::vector<uint8_t> dispatchCommandWithHandle(
    const std::vector<uint8_t>& /*command_proto*/,
    int64_t /*executor_handle*/) {
  static const std::vector<uint8_t> kNoProtoError = {0x00};
  return kNoProtoError;
}

std::vector<uint8_t> dispatchCommandWithHandleAndTimeout(
    const std::vector<uint8_t>& /*command_proto*/,
    int64_t /*executor_handle*/,
    int64_t /*timeout_ms*/) {
  static const std::vector<uint8_t> kNoProtoError = {0x00};
  return kNoProtoError;
}

void shutdownFlightResultServer() {
  // Proto support not available — no-op.
}

}  // namespace pangrova::jni

#endif  // PANGROVA_HAS_PROTO
