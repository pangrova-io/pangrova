// NativeEngineLifecycle: Manages the lifecycle of the Velox execution runtime
// with idempotent init/shutdown and safe re-init support.
//
// Owns all process-global Velox state: memory bridge, thread pool, UDF
// registry, and async data cache. Provides initialize()/shutdown() that
// can be called multiple times safely — enabling JVM fork reuse for E2E
// tests without SIGSEGV from stale global state.
//
// Key guarantees:
//   - initialize() after initialize() calls shutdown() first (clean re-init)
//   - shutdown() after shutdown() is a no-op (idempotent)
//   - Thread-safe: std::mutex guards all init/shutdown transitions
//   - The process owner drains registered executors during shutdown to release
//     all active task executor handles before destroying memory pools
//
// @ThreadSafe — all public methods are safe for concurrent access.
//
// Placement: velox/runtime/ — Velox runtime lifecycle management.
#pragma once

#include <jni.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// Forward declarations to avoid pulling heavy headers.
namespace folly {
class CPUThreadPoolExecutor;
class IOThreadPoolExecutor;
class Executor;
}  // namespace folly

namespace facebook::velox::cache {
class AsyncDataCache;
class SsdCache;
}  // namespace facebook::velox::cache

namespace pangrova::velox {
class VeloxMemoryGovernor;
}  // namespace pangrova::velox

namespace pangrova::udf {
class NativeUdfRegistry;
}  // namespace pangrova::udf

namespace pangrova::velox {
class TableFunctionRegistry;
}  // namespace pangrova::velox

namespace pangrova::memory {
class BlockAllocationListener;
}  // namespace pangrova::memory

namespace pangrova::capability {
class FunctionCatalogStartup;
}

namespace pangrova::jni {

// Forward declare NativeRuntimeConfig (defined in RuntimeInitializer.h).
struct NativeRuntimeConfig;

// Manages the full Velox runtime lifecycle: memory, functions, file systems,
// cache, connector, thread pool, and UDF registry.
//
// Usage:
//   auto& mgr = NativeEngineLifecycle::instance();
//   mgr.initialize(config, env, listener, drain);  // production init
//   // ... run queries ...
//   mgr.shutdown(drain);                          // clean teardown
//   mgr.initialize(config, env, listener, drain);  // safe re-init
class NativeEngineLifecycle {
 public:
  // Invoked inside the lifecycle lock, ahead of memory teardown on every
  // shutdown, reinitialization, and partial initialization rollback.
  using ExecutorDrain = void (&)();
  static NativeEngineLifecycle& instance();

  NativeEngineLifecycle(const NativeEngineLifecycle&) = delete;
  NativeEngineLifecycle& operator=(const NativeEngineLifecycle&) = delete;

  void initialize(const NativeRuntimeConfig& config,
                  JNIEnv* env, jobject allocationListener, ExecutorDrain drain);
  void initialize(const NativeRuntimeConfig& config, ExecutorDrain drain);
  void shutdown(ExecutorDrain drain);
  bool isInitialized() const;

  pangrova::velox::VeloxMemoryGovernor* veloxMemoryGovernor();
  facebook::velox::cache::AsyncDataCache* asyncDataCache();
  pangrova::udf::NativeUdfRegistry* udfRegistry();
  pangrova::velox::TableFunctionRegistry* tableFunctionRegistry();
  std::shared_ptr<folly::Executor> executor();
  std::shared_ptr<folly::Executor> ioExecutor();
  std::shared_ptr<folly::Executor> blockingIoExecutor();
  std::shared_ptr<capability::FunctionCatalogStartup> functionCatalogStartup();

 private:
  NativeEngineLifecycle() = default;
  struct FunctionRetirement;

  void initVeloxMemory(const NativeRuntimeConfig& config,
                       JNIEnv* env, jobject allocationListener);
  void initVeloxMemoryTestMode(const NativeRuntimeConfig& config);
  void initCommon(const NativeRuntimeConfig& config);
  void registerVeloxFunctions();
  void registerVeloxFileSystems(const NativeRuntimeConfig& config);
  void registerVeloxConnector(const NativeRuntimeConfig& config);
  void initAsyncDataCache(const NativeRuntimeConfig& config);
  void shutdownInternal(FunctionRetirement& retirement, ExecutorDrain drain);

  mutable std::mutex mutex_;
  std::atomic<bool> initialized_{false};

  std::unique_ptr<pangrova::velox::VeloxMemoryGovernor> velox_memory_governor_;
  std::unique_ptr<pangrova::memory::BlockAllocationListener> block_listener_;
  
  std::unique_ptr<pangrova::udf::NativeUdfRegistry> udf_registry_;
  std::shared_ptr<capability::FunctionCatalogStartup> function_catalog_startup_;
  std::unique_ptr<pangrova::velox::TableFunctionRegistry> table_function_registry_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  // Event-base threads only run timers and nonblocking continuations.
  std::shared_ptr<folly::IOThreadPoolExecutor> io_executor_;
  // Synchronous Flight Next() and filesystem reads must not occupy an event
  // base, or its timers and other exchange reads cannot make progress.
  std::shared_ptr<folly::CPUThreadPoolExecutor> blocking_io_executor_;

  bool velox_connector_registered_ = false;
  bool dwio_factories_registered_ = false;

  facebook::velox::cache::AsyncDataCache* async_data_cache_ = nullptr;
  std::shared_ptr<facebook::velox::cache::AsyncDataCache> async_data_cache_holder_;
  std::unique_ptr<facebook::velox::cache::SsdCache> ssd_cache_;
};

}  // namespace pangrova::jni
