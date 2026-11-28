// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "engine/velox/runtime/NativeEngineLifecycle.h"

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <stdexcept>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include "lance_c_ffi.h"

#include "jni/RuntimeInitializer.h"
#include "capabilities/artifact/ImmutableArtifactCache.h"
#include "capabilities/capability/BuiltinFunctionRegistration.h"
#include "capabilities/capability/FunctionCatalogPublication.h"
#include "engine/memory/PangrovaArbitrator.h"
#include "engine/memory/MemoryPressure.h"
#include "engine/velox/serde/ArrowBatchVectorSerde.h"
#include "indexing/text/bm25/TextSearchFunctions.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "engine/velox/exchange/PangrovaExchangeSource.h"
#include "engine/velox/memory/VeloxMemoryGovernor.h"
#include "engine/velox/plan/GenerateSeriesOperator.h"
#include "engine/velox/plan/LateralTableFunctionOperator.h"
#include "engine/velox/plan/RetrievalMergeOperator.h"
#include "engine/velox/plan/FilteredArtifactRetrievalOperator.h"
#include "engine/velox/plan/ArtifactBuildOperator.h"
#include "engine/velox/plan/GeneratedValuesOverlayOperator.h"
#include "engine/velox/runtime/TableFunctionRegistry.h"
#include "engine/memory/PangrovaAllocationListener.h"
#include "engine/memory/BlockAllocationListener.h"
#include "engine/memory/PangrovaArbitrator.h"
#include "udf/NativeUdfRegistry.h"
#include "engine/variant/VariantPathExpression.h"
#include "vector/diskann/DiskAnnIndexCache.h"
#include "vector/VectorDistanceFunctions.h"

// Velox connector registry.
#include "velox/connectors/Connector.h"

// Velox AsyncDataCache and memory allocator headers.
#include "engine/compat/NativeFeatureDetection.h"

// Velox memory headers for cache integration (Memory.h needed beyond what
// NativeFeatureDetection provides for MallocAllocator).
#if PANGROVA_HAS_VELOX_MEMORY_ALLOCATOR
#include "velox/common/memory/Memory.h"
#endif

// Velox filesystem registration headers.
#if __has_include("velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h")
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#define PANGROVA_HAS_VELOX_S3FS 1
#else
#define PANGROVA_HAS_VELOX_S3FS 0
#endif

#if __has_include("velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h")
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#define PANGROVA_HAS_VELOX_HDFSFS 1
#else
#define PANGROVA_HAS_VELOX_HDFSFS 0
#endif

#if __has_include("velox/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h")
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h"
#define PANGROVA_HAS_VELOX_GCSFS 1
#else
#define PANGROVA_HAS_VELOX_GCSFS 0
#endif

#if __has_include("velox/common/file/FileSystems.h")
#include "velox/common/file/FileSystems.h"
#define PANGROVA_HAS_VELOX_LOCALFS 1
#else
#define PANGROVA_HAS_VELOX_LOCALFS 0
#endif

#if __has_include("velox/serializers/PrestoSerializer.h")
#include "velox/serializers/PrestoSerializer.h"
#define PANGROVA_HAS_PRESTO_SERDE 1
#else
#define PANGROVA_HAS_PRESTO_SERDE 0
#endif

#if __has_include("velox/functions/prestosql/registration/RegistrationFunctions.h")
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#define PANGROVA_HAS_VELOX_SCALAR_FUNCS 1
#else
#define PANGROVA_HAS_VELOX_SCALAR_FUNCS 0
#endif

#if __has_include("velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h")
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#define PANGROVA_HAS_VELOX_AGG_FUNCS 1
#else
#define PANGROVA_HAS_VELOX_AGG_FUNCS 0
#endif

#if __has_include("velox/functions/prestosql/window/WindowFunctionsRegistration.h")
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#define PANGROVA_HAS_VELOX_WINDOW_FUNCS 1
#else
#define PANGROVA_HAS_VELOX_WINDOW_FUNCS 0
#endif

#if __has_include("velox/dwio/parquet/RegisterParquetReader.h")
#include "velox/dwio/parquet/RegisterParquetReader.h"
#define PANGROVA_HAS_VELOX_PARQUET_READER 1
#else
namespace facebook::velox::parquet {
void registerParquetReaderFactory();
}
#define PANGROVA_HAS_VELOX_PARQUET_READER 1
#endif

#if __has_include("velox/dwio/dwrf/RegisterDwrfReader.h")
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#define PANGROVA_HAS_VELOX_DWRF_READER 1
#else
namespace facebook::velox::dwrf {
void registerDwrfReaderFactory();
}
#define PANGROVA_HAS_VELOX_DWRF_READER 1
#endif

namespace pangrova::jni {

struct NativeEngineLifecycle::FunctionRetirement final {
  std::vector<std::shared_ptr<const capability::FunctionCatalogPublication>> publications;
  std::vector<std::unique_ptr<pangrova::udf::NativeUdfRegistry>> registries;

  void retire() {
    // Both owners retain callback factories during exact SDK-entry removal.
    // The enclosing lifecycle lock has already been released.
    for (const auto& registry : registries) registry->retireOwnedRegistrations();
  }
};

NativeEngineLifecycle& NativeEngineLifecycle::instance() {
  static NativeEngineLifecycle inst;
  return inst;
}

void NativeEngineLifecycle::initialize(const NativeRuntimeConfig& config,
                                       JNIEnv* env, jobject allocationListener, ExecutorDrain drain) {
  FunctionRetirement retirement;
  std::unique_lock<std::mutex> lock(mutex_);
  if (process_finalized_) {
    throw std::logic_error("Native process resources have been finalized");
  }
  if (initialized_.load(std::memory_order_relaxed)) {
    shutdownInternal(retirement, drain);
  }
  try {
    initVeloxMemory(config, env, allocationListener);
    initCommon(config);
  } catch (...) {
    // Runtime initialization is one atomic readiness transition. Tear down all
    // partially-created pools, caches, registrations and memory listeners so a
    // failed node can be repaired and retried without process restart.
    shutdownInternal(retirement, drain);
    lock.unlock();
    retirement.retire();
    throw;
  }
  initialized_.store(true, std::memory_order_release);
  lock.unlock();
  retirement.retire();

  std::cerr << "level=INFO event=native_engine_lifecycle_initialized"
            << " memory_bytes=" << config.max_memory_bytes
            << " io_threads=" << config.io_thread_count
            << " cache_enabled=" << (config.cache_enabled ? "true" : "false")
            << std::endl;
}

void NativeEngineLifecycle::initialize(const NativeRuntimeConfig& config, ExecutorDrain drain) {
  FunctionRetirement retirement;
  std::unique_lock<std::mutex> lock(mutex_);
  if (process_finalized_) {
    throw std::logic_error("Native process resources have been finalized");
  }
  if (initialized_.load(std::memory_order_relaxed)) {
    shutdownInternal(retirement, drain);
  }
  try {
    initVeloxMemoryTestMode(config);
    initCommon(config);
  } catch (...) {
    shutdownInternal(retirement, drain);
    lock.unlock();
    retirement.retire();
    throw;
  }
  initialized_.store(true, std::memory_order_release);
  lock.unlock();
  retirement.retire();

  std::cerr << "level=INFO event=native_engine_lifecycle_initialized_test_mode"
            << " memory_bytes=" << config.max_memory_bytes
            << std::endl;
}

void NativeEngineLifecycle::shutdown(ExecutorDrain drain) {
  FunctionRetirement retirement;
  std::unique_lock<std::mutex> lock(mutex_);
  if (!initialized_.load(std::memory_order_relaxed)) {
    return;
  }
  shutdownInternal(retirement, drain);
  lock.unlock();
  retirement.retire();
}

void NativeEngineLifecycle::finalizeProcess() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (initialized_.load(std::memory_order_relaxed)) {
    throw std::logic_error("Native runtime must shut down before process finalization");
  }
  if (process_finalized_) {
    return;
  }
#if PANGROVA_HAS_VELOX_S3FS
  // Velox finalization destroys cached clients and permanently closes the AWS
  // SDK. Run it before static destruction, once all task owners have exited.
  facebook::velox::filesystems::finalizeS3FileSystem();
#endif
  process_finalized_ = true;
}

std::shared_ptr<capability::FunctionCatalogStartup>
NativeEngineLifecycle::functionCatalogStartup() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!initialized_.load(std::memory_order_relaxed) || !function_catalog_startup_) {
    throw std::logic_error("Function catalogue startup requires the initialized runtime incarnation");
  }
  return function_catalog_startup_;
}

bool NativeEngineLifecycle::isInitialized() const {
  return initialized_.load(std::memory_order_acquire);
}


pangrova::velox::VeloxMemoryGovernor* NativeEngineLifecycle::veloxMemoryGovernor() {
  std::lock_guard<std::mutex> lock(mutex_);
  return velox_memory_governor_.get();
}

facebook::velox::cache::AsyncDataCache*
NativeEngineLifecycle::asyncDataCache() {
  std::lock_guard<std::mutex> lock(mutex_);
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
  return async_data_cache_;
#else
  return nullptr;
#endif
}

pangrova::udf::NativeUdfRegistry* NativeEngineLifecycle::udfRegistry() {
  std::lock_guard<std::mutex> lock(mutex_);
  return udf_registry_.get();
}

pangrova::velox::TableFunctionRegistry* NativeEngineLifecycle::tableFunctionRegistry() {
  std::lock_guard<std::mutex> lock(mutex_);
  return table_function_registry_.get();
}

std::shared_ptr<folly::Executor> NativeEngineLifecycle::executor() {
  std::lock_guard<std::mutex> lock(mutex_);
  return executor_;
}

std::shared_ptr<folly::Executor> NativeEngineLifecycle::ioExecutor() {
  std::lock_guard<std::mutex> lock(mutex_);
  return io_executor_;
}

std::shared_ptr<folly::Executor> NativeEngineLifecycle::blockingIoExecutor() {
  std::lock_guard<std::mutex> lock(mutex_);
  return blocking_io_executor_;
}

void NativeEngineLifecycle::initVeloxMemory(
    const NativeRuntimeConfig& config,
    JNIEnv* env, jobject allocationListener) {
  pangrova::velox::VeloxMemoryGovernorConfig governor_config;
  governor_config.arbitrator_capacity = config.max_memory_bytes;
  // L1-8: When arbitrator_reserved_capacity is 0 (not explicitly set from
  // Java), compute a default 5% of total memory as reserved capacity.
  // This prevents sudden large queries from triggering cascading spills.
  if (config.arbitrator_reserved_capacity > 0) {
    governor_config.reserved_capacity = config.arbitrator_reserved_capacity;
  } else if (config.max_memory_bytes > 0) {
    governor_config.reserved_capacity = config.max_memory_bytes / 20;  // 5%
  } else {
    governor_config.reserved_capacity = 0;
  }
  governor_config.initial_query_capacity = config.query_initial_capacity;
  governor_config.enable_spill = config.spill_enabled;
  governor_config.spill_directory = config.spill_directory;
  governor_config.max_spill_bytes = config.max_spill_bytes;

  if (config.spill_enabled && !config.spill_directory.empty()) {
    std::filesystem::create_directories(config.spill_directory);
  }

  auto pangrova_listener =
      std::make_unique<pangrova::memory::PangrovaAllocationListener>(
          env, allocationListener, config.non_jvm_memory_cap_bytes);
  block_listener_ =
      std::make_unique<pangrova::memory::BlockAllocationListener>(
          std::move(pangrova_listener));

  velox_memory_governor_ = std::make_unique<pangrova::velox::VeloxMemoryGovernor>(
      config.max_memory_bytes, block_listener_.get(), governor_config);
}

void NativeEngineLifecycle::initVeloxMemoryTestMode(
    const NativeRuntimeConfig& config) {
  pangrova::velox::VeloxMemoryGovernorConfig governor_config;
  governor_config.arbitrator_capacity = config.max_memory_bytes;
  governor_config.enable_spill = config.spill_enabled;
  governor_config.spill_directory = config.spill_directory;
  governor_config.max_spill_bytes = config.max_spill_bytes;

  velox_memory_governor_ = std::make_unique<pangrova::velox::VeloxMemoryGovernor>(
      config.max_memory_bytes, /*listener=*/nullptr, governor_config);
}

void NativeEngineLifecycle::registerVeloxFunctions() {
  pangrova::capability::registerBuiltinFunctions();

#if PANGROVA_HAS_PRESTO_SERDE
  if (!facebook::velox::isRegisteredVectorSerde()) {
    facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
#endif

  // The default SDK serde above is used for SDK-internal spill only. Every
  // Pangrova result and Exchange selects this sole named wire serde; unsupported
  // values fail before payload publication, without another execution path.
  if (!facebook::velox::isRegisteredNamedVectorSerde(
          pangrova::velox::ArrowBatchVectorSerde::kSerdeKind)) {
    pangrova::velox::ArrowBatchVectorSerde::registerNamedVectorSerde();
  }

  pangrova::velox::PangrovaExchangeSource::registerExchangeSourceFactory();

  // Register custom table function operators with Velox's execution framework.
  pangrova::velox::registerGenerateSeriesOperator();
  pangrova::velox::registerLateralTableFunctionOperator();
  pangrova::velox::registerRetrievalMergeOperator();
  pangrova::velox::registerFilteredArtifactRetrievalOperator();
  pangrova::velox::registerArtifactBuildOperator();
  pangrova::velox::registerGeneratedValuesOverlayOperator();

  bool dwio_fresh = !dwio_factories_registered_;
  if (dwio_fresh) {
#if PANGROVA_HAS_VELOX_PARQUET_READER
    facebook::velox::parquet::registerParquetReaderFactory();
#endif
#if PANGROVA_HAS_VELOX_DWRF_READER
    facebook::velox::dwrf::registerDwrfReaderFactory();
#endif
    dwio_factories_registered_ = true;
  }

  std::cerr << "level=INFO event=velox_functions_registered"
            << " scalar=" << PANGROVA_HAS_VELOX_SCALAR_FUNCS
            << " aggregate=" << PANGROVA_HAS_VELOX_AGG_FUNCS
            << " window=" << PANGROVA_HAS_VELOX_WINDOW_FUNCS
            << " presto_serde=" << PANGROVA_HAS_PRESTO_SERDE
            << " parquet_reader=" << PANGROVA_HAS_VELOX_PARQUET_READER
            << " dwrf_reader=" << PANGROVA_HAS_VELOX_DWRF_READER
            << " dwio_fresh_register=" << (dwio_fresh ? "true" : "false")
            << std::endl;
}

void NativeEngineLifecycle::registerVeloxFileSystems(
    const NativeRuntimeConfig& config) {
#if PANGROVA_HAS_VELOX_LOCALFS
  facebook::velox::filesystems::registerLocalFileSystem();
#endif
#if PANGROVA_HAS_VELOX_S3FS
  facebook::velox::filesystems::registerS3FileSystem(
      [](const std::shared_ptr<const facebook::velox::config::ConfigBase>& properties, std::string_view path) {
        if (!properties) throw std::invalid_argument("Native S3 filesystem requires storage settings");
        constexpr std::size_t prefix_length = sizeof("s3://") - 1;
        const auto bucket = path.substr(prefix_length, path.find('/', prefix_length) - prefix_length);
        const auto authority = properties->get<std::string>("pangrova.s3.authority");
        if (authority) return authority.value() + ":" + std::string(bucket);
        return facebook::velox::filesystems::S3Config::cacheKey(bucket, properties);
      });
#endif
#if PANGROVA_HAS_VELOX_HDFSFS
  facebook::velox::filesystems::registerHdfsFileSystem();
#endif
#if PANGROVA_HAS_VELOX_GCSFS
  facebook::velox::filesystems::registerGcsFileSystem();
#endif

  std::cerr << "level=INFO event=velox_filesystems_registered"
            << " local=" << PANGROVA_HAS_VELOX_LOCALFS
            << " s3=" << PANGROVA_HAS_VELOX_S3FS
            << " hdfs=" << PANGROVA_HAS_VELOX_HDFSFS
            << " gcs=" << PANGROVA_HAS_VELOX_GCSFS
            << std::endl;

  (void)config;
}

void NativeEngineLifecycle::registerVeloxConnector(
    const NativeRuntimeConfig& config) {
  if (velox_connector_registered_) {
    return;
  }
  auto fsConfigMap = buildFileSystemConfigMap(config);
  std::shared_ptr<const facebook::velox::config::ConfigBase> connectorConfig;
  if (!fsConfigMap.empty()) {
    connectorConfig = std::make_shared<facebook::velox::config::ConfigBase>(
        std::move(fsConfigMap));
  }
  auto connector = std::make_shared<pangrova::velox::PangrovaVeloxConnector>(
      pangrova::velox::PangrovaVeloxConnector::kConnectorId,
      std::move(connectorConfig));
  try {
    facebook::velox::connector::registerConnector(std::move(connector));
  } catch (const std::exception& e) {
    // Velox throws if connector ID is already registered (expected on re-init).
    // Only suppress "already registered" errors; rethrow genuine failures.
    std::string msg = e.what();
    if (msg.find("already registered") != std::string::npos ||
        msg.find("Already registered") != std::string::npos) {
      std::cerr << "level=DEBUG event=connector_register_skipped"
                << " reason=" << msg << std::endl;
    } else {
      throw;
    }
  }
  velox_connector_registered_ = true;
}

void NativeEngineLifecycle::initAsyncDataCache(
    const NativeRuntimeConfig& config) {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE && PANGROVA_HAS_VELOX_MEMORY_ALLOCATOR
  if (!velox_memory_governor_ || !velox_memory_governor_->veloxMemoryManager()) {
    std::cerr << "level=ERROR event=velox_async_data_cache_init_failed"
              << " reason=memory_governor_not_initialized" << std::endl;
    return;
  }
  auto* allocator = velox_memory_governor_->veloxMemoryManager()->allocator();

  if (config.ssd_cache_bytes > 0 && !config.ssd_cache_path.empty()) {
    std::filesystem::create_directories(config.ssd_cache_path);
    // A zero checkpoint interval disables checkpointing entirely, so the SSD
    // index was lost on every restart and previously cached blocks could not be
    // reused. Checkpoint as the cache grows instead.
    constexpr uint64_t kSsdCheckpointIntervalBytes = 64ULL * 1024 * 1024;
    facebook::velox::cache::SsdCache::Config ssdConfig(
        config.ssd_cache_path,
        config.ssd_cache_bytes,
        /*numShards=*/4,
        /*executor=*/executor_.get(),
        kSsdCheckpointIntervalBytes);
    ssd_cache_ =
        std::make_unique<facebook::velox::cache::SsdCache>(ssdConfig);
  }

  auto cache = facebook::velox::cache::AsyncDataCache::create(
      allocator, std::move(ssd_cache_));

  facebook::velox::cache::AsyncDataCache::setInstance(cache.get());
  async_data_cache_holder_ = std::move(cache);
  async_data_cache_ =
      facebook::velox::cache::AsyncDataCache::getInstance();

  // Wire cache into memory governor for MemorySnapshot reporting.
  if (velox_memory_governor_) {
    velox_memory_governor_->setAsyncDataCache(async_data_cache_);
  }

  // AsyncDataCache draws from the supplied allocator, so ram_cache_bytes is not
  // an independent quota; the allocator capacity is the real bound. Report both
  // so an operator can see when the configured value is not the effective one.
  const auto allocator_capacity =
      static_cast<uint64_t>(allocator->capacity());
  std::cerr << "level=INFO event=velox_async_data_cache_initialized"
            << " ram_cache_bytes=" << config.ram_cache_bytes
            << " effective_allocator_capacity_bytes=" << allocator_capacity
            << " ssd_cache_bytes=" << config.ssd_cache_bytes
            << std::endl;
  if (config.ram_cache_bytes > 0
      && static_cast<uint64_t>(config.ram_cache_bytes) > allocator_capacity) {
    std::cerr << "level=WARN event=ram_cache_bytes_exceeds_allocator"
              << " ram_cache_bytes=" << config.ram_cache_bytes
              << " allocator_capacity_bytes=" << allocator_capacity
              << " effective_bound=allocator_capacity" << std::endl;
  }
#else
  (void)config;
#endif
}

void NativeEngineLifecycle::initCommon(const NativeRuntimeConfig& config) {
  // Lance is part of the mandatory Native data plane. A Worker must never
  // become ready and advertise execution when the linked Rust ABI or its
  // process runtime is unusable.
  if (lance_ffi_abi_version() != PANGROVA_LANCE_C_FFI_ABI_VERSION) {
    throw std::runtime_error("Lance C FFI ABI version mismatch");
  }
  if (lance_ffi_runtime_init() != 0) {
    const char* error = lance_ffi_last_error();
    throw std::runtime_error(
        std::string("Lance FFI runtime initialization failed") +
        (error == nullptr ? "" : std::string(": ") + error));
  }

  if (config.pressure_threshold_low > 0 &&
      config.pressure_threshold_medium > 0 &&
      config.pressure_threshold_high > 0 &&
      config.pressure_threshold_critical > 0) {
    pangrova::memory::configurePressureThresholds(
        config.pressure_threshold_low,
        config.pressure_threshold_medium,
        config.pressure_threshold_high,
        config.pressure_threshold_critical);
  }

  registerVeloxFunctions();

  udf_registry_ = std::make_unique<pangrova::udf::NativeUdfRegistry>();
  function_catalog_startup_ = std::make_shared<capability::FunctionCatalogStartup>();

  table_function_registry_ = std::make_unique<pangrova::velox::TableFunctionRegistry>();
  table_function_registry_->registerBuiltins();

  registerVeloxFileSystems(config);

  // Split the thread pool into two pools:
  //   1. task_executor_: handles PlanExecutionDriver::executeAsync() blocking waits
  //   2. driver_executor_: handles Velox Task::start() Driver execution
  //
  // This separation prevents deadlock where all threads block in
  // waitForCompletion() leaving none for Velox Drivers.
  int32_t task_threads = config.executor_task_threads > 0
      ? config.executor_task_threads
      : std::max(config.num_drivers * 2, 16);
  int32_t driver_threads = config.executor_driver_threads > 0
      ? config.executor_driver_threads
      : std::max(config.num_drivers * 4, 32);
  executor_ =
      std::make_shared<folly::CPUThreadPoolExecutor>(task_threads + driver_threads);
  io_executor_ = std::make_shared<folly::IOThreadPoolExecutor>(
      std::max(config.io_thread_count, 1));
  // A multistage query can have dozens of exchange sources waiting for their
  // producers. Keep these synchronous reads off the event-base pool and
  // reserve enough workers for upstream stages to make forward progress.
  blocking_io_executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::max(std::min(config.io_thread_count, 16) * 16, 64));

  if (config.cache_enabled) {
    initAsyncDataCache(config);
    pangrova::artifact::ImmutableArtifactCache::instance().configure(
        config.artifact_cache_path,
        static_cast<uint64_t>(config.artifact_cache_bytes));
    pangrova::vector::diskann::DiskAnnIndexCache::instance().configure(
        static_cast<uint64_t>(config.artifact_index_memory_bytes));
  }

  registerVeloxConnector(config);
}

void NativeEngineLifecycle::shutdownInternal(FunctionRetirement& retirement, ExecutorDrain drain) {
  drain();

  // Handle destruction closes exchange sources and cancels their Flight RPCs.
  // Drain the blocking-I/O pool before the CPU pool and memory bridge so no
  // late network callback can enqueue a page into a destroyed Velox task.
  if (blocking_io_executor_) {
    blocking_io_executor_->join();
    blocking_io_executor_.reset();
  }
  if (io_executor_) {
    io_executor_->join();
    io_executor_.reset();
  }

  // Join the CPU executor BEFORE destroying the memory bridge. Velox Task
  // destruction may happen asynchronously on executor threads; those
  // destructors reference memory pools owned by the bridge.
  if (executor_) {
    executor_->join();
    executor_.reset();
  }

  if (function_catalog_startup_) {
    retirement.publications.push_back(function_catalog_startup_->close());
    function_catalog_startup_.reset();
  }
  if (udf_registry_) retirement.registries.push_back(std::move(udf_registry_));

  // Handles and executors no longer use Lance. Drop the process-wide Tokio
  // reference before static destruction; any independently open FFI handle
  // retains its own Arc until that handle is closed.
  lance_ffi_runtime_shutdown();

#if PANGROVA_HAS_VELOX_ASYNC_CACHE
  async_data_cache_ = nullptr;
  facebook::velox::cache::AsyncDataCache::setInstance(nullptr);
  // Velox's allocator retains a strong reference from registerCache().
  // Dropping our reference alone leaves its page allocations alive until the
  // MemoryManager is destroyed, after the shutdown leak check.
  if (async_data_cache_holder_) {
    async_data_cache_holder_->shutdown();
  }
  async_data_cache_holder_.reset();
  ssd_cache_.reset();
#endif

  if (pangrova::vector::diskann::DiskAnnIndexCache::instance().enabled()) {
    pangrova::vector::diskann::DiskAnnIndexCache::instance().shutdown();
  }
  if (pangrova::artifact::ImmutableArtifactCache::instance().enabled()) {
    pangrova::artifact::ImmutableArtifactCache::instance().shutdown();
  }

  if (velox_memory_governor_ && velox_memory_governor_->veloxMemoryManager()) {
    int64_t used = velox_memory_governor_->usedMemory();
    if (used > 0) {
      std::cerr << "level=WARN event=memory_leak_detected_on_shutdown"
                << " used_bytes=" << used
                << " pool_tree=" << velox_memory_governor_->veloxMemoryManager()->toString(true)
                << std::endl;
    }
  }

  velox_memory_governor_.reset();

  facebook::velox::memory::MemoryManager::testingSetInstance({});

  // Only unregister the PANGROVA arbitrator factory if it was registered.
  // In test-mode initialization (listener_ == nullptr), VeloxMemoryGovernor
  // uses Velox's default SharedArbitrator and never registers the PANGROVA
  // factory. Calling unregisterFactory on an unregistered kind throws.
  if (block_listener_) {
    facebook::velox::memory::MemoryArbitrator::unregisterFactory(
        pangrova::memory::PangrovaArbitrator::kKind);
  }

  // Unregister the Pangrova connector to allow clean re-registration on re-init.
  if (velox_connector_registered_) {
    try {
      facebook::velox::connector::unregisterConnector(
          pangrova::velox::PangrovaVeloxConnector::kConnectorId);
    } catch (const std::exception& e) {
      // Velox may not support unregisterConnector in all versions.
      std::cerr << "level=WARN event=connector_unregister_skipped"
                << " reason=" << e.what() << std::endl;
    }
  }

  block_listener_.reset();
  table_function_registry_.reset();

  velox_connector_registered_ = false;
  dwio_factories_registered_ = false;

  initialized_.store(false, std::memory_order_release);

  std::cerr << "level=INFO event=native_engine_lifecycle_shutdown" << std::endl;
}

}  // namespace pangrova::jni
