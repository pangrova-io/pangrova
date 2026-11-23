#include "jni/JniEntryPoints.h"

#include <cstdio>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

#include "engine/columnar/ColumnarDataExporter.h"
#include "capabilities/capability/FunctionCatalogPublication.h"
#include "jni/callback/GlobalEgressRegistry.h"
#include "jni/callback/GlobalWriterRegistry.h"
#include "command/flight/FlightServerRegistry.h"
#include "command/NativeCommandDispatcher.h"
#include "command/StatusSerializer.h"
#include "command/task/TaskRequestParser.h"
#include "jni/RuntimeInitializer.h"
#include "command/resource/HandleRegistry.h"
#include "engine/observability/NativeTracer.h"
#include "engine/observability/PangrovaLog.h"
#include "engine/observability/SpanScope.h"
#include "engine/shuffle/ShuffleEgressExecutor.h"
#include "engine/shuffle/ShuffleTransferLimits.h"
#include "engine/shuffle/ShuffleTicketKeys.h"
#include "udf/JvmScalarFunction.h"
#include "engine/velox/exchange/PangrovaExchangeSource.h"
#include "engine/velox/memory/VeloxMemoryGovernor.h"
#include "engine/velox/runtime/NativeEngineLifecycle.h"

#include "pangrova/protocol/v2/runtime.pb.h"

#define JNI_CATCH_AND_THROW(env)                                       \
  catch (const std::exception& e) {                                    \
    pangrova::jni::throwJniCallException(env, e.what());            \
  }                                                                    \
  catch (...) {                                                        \
    pangrova::jni::throwJniCallException(env, "Unknown C++ error"); \
  }

namespace pangrova::jni {

// ---------------------------------------------------------------------------
// Global JavaVM* — cached in JNI_OnLoad per pangrova-rules §9.
// ---------------------------------------------------------------------------
namespace {
std::atomic<JavaVM*> g_cached_jvm{nullptr};

// A Native reader or exported Arrow buffer can exit on a non-Java thread.
class ResultDrainJniEnv {
 public:
  explicit ResultDrainJniEnv(JavaVM* vm) : vm_(vm) {
    const auto status = vm_->GetEnv(reinterpret_cast<void**>(&env_), JNI_VERSION_1_8);
    if (status == JNI_EDETACHED) {
      attached_ = vm_->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env_), nullptr) == JNI_OK;
      if (!attached_) env_ = nullptr;
    } else if (status != JNI_OK) {
      env_ = nullptr;
    }
  }

  ResultDrainJniEnv(const ResultDrainJniEnv&) = delete;
  ResultDrainJniEnv& operator=(const ResultDrainJniEnv&) = delete;

  ~ResultDrainJniEnv() { if (attached_) vm_->DetachCurrentThread(); }
  JNIEnv* get() const { return env_; }

 private:
  JavaVM* vm_;
  JNIEnv* env_ = nullptr;
  bool attached_ = false;
};

class ResultDrainUpcall {
 public:
  ResultDrainUpcall(JNIEnv* env, jobject runnable, int64_t handle)
      : vm_(globalJavaVM()), run_(globalJniCache().runnable_run), handle_(handle) {
    if (!vm_ || !run_ || !runnable) throw std::invalid_argument("Result drain callback is unavailable");
    runnable_ = env->NewGlobalRef(runnable);
    if (!runnable_) throw std::runtime_error("Could not retain result drain callback");
  }

  ResultDrainUpcall(const ResultDrainUpcall&) = delete;
  ResultDrainUpcall& operator=(const ResultDrainUpcall&) = delete;

  ~ResultDrainUpcall() {
    if (!runnable_) return;
    ResultDrainJniEnv thread(vm_);
    if (auto* env = thread.get()) {
      env->DeleteGlobalRef(runnable_);
    } else {
      PANGROVA_LOG_ERROR << "event=result_drain_callback_release_failed executor_handle=" << handle_;
    }
  }

  void notify() noexcept {
    ResultDrainJniEnv thread(vm_);
    auto* env = thread.get();
    if (!env) {
      PANGROVA_LOG_ERROR << "event=result_drain_callback_attach_failed executor_handle=" << handle_;
      return;
    }
    env->CallVoidMethod(runnable_, run_);
    if (env->ExceptionCheck()) {
      PANGROVA_LOG_ERROR << "event=result_drain_callback_failed executor_handle=" << handle_;
      env->ExceptionDescribe();
      env->ExceptionClear();
    }
    env->DeleteGlobalRef(runnable_);
    runnable_ = nullptr;
  }

 private:
  JavaVM* vm_;
  jmethodID run_;
  int64_t handle_;
  jobject runnable_ = nullptr;
};

std::shared_ptr<pangrova::executor::TaskExecutor> constructTaskExecutor(
    JNIEnv* env, pangrova::executor::TaskExecutorConfig config) {
  if (config.shuffle_execution.has_value()) {
    jobject egress = GlobalEgressRegistry::instance().acquire(env);
    if (!egress) {
      throw std::runtime_error(
          "External shuffle task requires a registered Worker egress");
    }
    try {
      config.shuffle_egress =
          std::make_shared<pangrova::shuffle::ShuffleEgressExecutor>(
              env, egress);
      env->DeleteLocalRef(egress);
    } catch (...) {
      env->DeleteLocalRef(egress);
      throw;
    }
  }
  return std::make_shared<pangrova::executor::TaskExecutor>(std::move(config));
}

int64_t createTaskExecutor(JNIEnv* env, protocol::v2::TaskRequest request,
                          const std::string& trace_id = {}, const std::string& span_id = {}) {
  auto& registry = HandleRegistry::instance();
  const auto handle = registry.reserveCreation(creationIdentity(request));
  try {
    auto config = taskConfigFromRequest(request);
    config.function_catalog = NativeEngineLifecycle::instance()
        .functionCatalogStartup()->requirePublished();
    config.function_registry = config.function_catalog->registry();
    config.trace_id = trace_id;
    config.span_id = span_id;
    registry.attachCreation(handle, constructTaskExecutor(env, std::move(config)));
  } catch (...) {
    // All constructor locals have unwound before this exit is recorded. An
    // attached executor remains owned by the same registry for actual release.
    registry.finishCreation(handle);
    throw;
  }
  registry.finishCreation(handle);
  return handle;
}
}  // namespace

JavaVM* globalJavaVM() {
  return g_cached_jvm.load(std::memory_order_acquire);
}

void cacheJavaVM(JavaVM* vm) {
  g_cached_jvm.store(vm, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// JNI Reference Cache — singleton + init/release
// ---------------------------------------------------------------------------
namespace {
std::mutex g_jni_cache_mutex;
JniReferenceCache g_jni_cache;
bool g_jni_cache_initialized = false;

// Helper: FindClass + NewGlobalRef, throws on failure.
jclass findAndCacheClass(JNIEnv* env, const char* name) {
  jclass local = env->FindClass(name);
  if (!local) {
    throw std::runtime_error(
        std::string("JNI FindClass failed for: ") + name);
  }
  auto global = static_cast<jclass>(env->NewGlobalRef(local));
  env->DeleteLocalRef(local);
  if (!global) {
    throw std::runtime_error(
        std::string("JNI NewGlobalRef failed for: ") + name);
  }
  return global;
}

// Helper: delete a global ref and null the pointer (idempotent).
void deleteGlobalRefSafe(JNIEnv* env, jclass& ref) {
  if (ref) {
    env->DeleteGlobalRef(ref);
    ref = nullptr;
  }
}
}  // anonymous namespace

void initJniReferenceCache(JNIEnv* env) {
  std::lock_guard<std::mutex> lock(g_jni_cache_mutex);
  if (g_jni_cache_initialized) {
    return;  // Idempotent.
  }

  // P1-BUG-5: Use try-catch to clean up partially-initialized GlobalRefs
  // if any FindClass/GetMethodID fails mid-initialization.
  try {
    // Cache jclass references via NewGlobalRef.
    g_jni_cache.native_bridge_class =
        findAndCacheClass(env, "org/pangrova/worker/jni/NativeRuntime");
    g_jni_cache.native_exception_class =
        findAndCacheClass(env, "org/pangrova/worker/exception/JniCallException");
    g_jni_cache.arrow_array_class =
        findAndCacheClass(env, "org/apache/arrow/c/ArrowArray");
    g_jni_cache.arrow_schema_class =
        findAndCacheClass(env, "org/apache/arrow/c/ArrowSchema");
    g_jni_cache.runnable_class = findAndCacheClass(env, "java/lang/Runnable");
    g_jni_cache.runnable_run = env->GetMethodID(g_jni_cache.runnable_class, "run", "()V");
    if (!g_jni_cache.runnable_run) throw std::runtime_error("JNI GetMethodID failed for Runnable.run()");

    // Cache jmethodID values (stable for class lifetime).
    g_jni_cache.exception_constructor = env->GetMethodID(
        g_jni_cache.native_exception_class, "<init>",
        "(Ljava/lang/String;)V");
    if (!g_jni_cache.exception_constructor) {
      throw std::runtime_error(
          "JNI GetMethodID failed for JniCallException(String)");
    }

    g_jni_cache.arrow_array_address = env->GetMethodID(
        g_jni_cache.arrow_array_class, "memoryAddress", "()J");
    if (!g_jni_cache.arrow_array_address) {
      throw std::runtime_error(
          "JNI GetMethodID failed for ArrowArray.memoryAddress()");
    }

    g_jni_cache.arrow_schema_address = env->GetMethodID(
        g_jni_cache.arrow_schema_class, "memoryAddress", "()J");
    if (!g_jni_cache.arrow_schema_address) {
      throw std::runtime_error(
          "JNI GetMethodID failed for ArrowSchema.memoryAddress()");
    }

    g_jni_cache_initialized = true;
  } catch (...) {
    // Clean up any GlobalRefs that were successfully created before failure.
    deleteGlobalRefSafe(env, g_jni_cache.native_bridge_class);
    deleteGlobalRefSafe(env, g_jni_cache.native_exception_class);
    deleteGlobalRefSafe(env, g_jni_cache.arrow_array_class);
    deleteGlobalRefSafe(env, g_jni_cache.arrow_schema_class);
    deleteGlobalRefSafe(env, g_jni_cache.runnable_class);
    g_jni_cache.runnable_run = nullptr;
    g_jni_cache.exception_constructor = nullptr;
    g_jni_cache.arrow_array_address = nullptr;
    g_jni_cache.arrow_schema_address = nullptr;
    throw;
  }
}

void releaseJniReferenceCache(JNIEnv* env) {
  std::lock_guard<std::mutex> lock(g_jni_cache_mutex);
  if (!g_jni_cache_initialized) {
    return;  // Nothing to release.
  }

  // Release all global refs. jmethodID values do not need release.
  deleteGlobalRefSafe(env, g_jni_cache.native_bridge_class);
  deleteGlobalRefSafe(env, g_jni_cache.native_exception_class);
  deleteGlobalRefSafe(env, g_jni_cache.arrow_array_class);
  deleteGlobalRefSafe(env, g_jni_cache.arrow_schema_class);
  deleteGlobalRefSafe(env, g_jni_cache.runnable_class);

  // Zero out method IDs for safety.
  g_jni_cache.exception_constructor = nullptr;
  g_jni_cache.arrow_array_address = nullptr;
  g_jni_cache.arrow_schema_address = nullptr;
  g_jni_cache.runnable_run = nullptr;

  g_jni_cache_initialized = false;
}

JniReferenceCache& globalJniCache() {
  return g_jni_cache;
}

// ---------------------------------------------------------------------------
// Exception helpers
// ---------------------------------------------------------------------------

void throwJavaException(JNIEnv* env, const char* className,
                        const char* message) {
  jclass exClass = env->FindClass(className);
  if (exClass != nullptr) {
    env->ThrowNew(exClass, message);
    env->DeleteLocalRef(exClass);
  }
}

void throwJniCallException(JNIEnv* env, const char* message) {
  // Preserve the original JVM allocation/lookup failure. JNI exception
  // construction must not invoke allocation while that exception is pending.
  if (env->ExceptionCheck()) return;
  // Fast path: use cached class + constructor to avoid FindClass overhead.
  auto& cache = globalJniCache();
  if (cache.native_exception_class && cache.exception_constructor) {
    jstring jmsg = env->NewStringUTF(message);
    if (jmsg) {
      jthrowable ex = static_cast<jthrowable>(env->NewObject(
          cache.native_exception_class, cache.exception_constructor, jmsg));
      if (ex) {
        env->Throw(ex);
        env->DeleteLocalRef(ex);
      }
      env->DeleteLocalRef(jmsg);
      return;
    }
  }
  // Fallback: cache not yet initialized or NewStringUTF failed.
  throwJavaException(env, "org/pangrova/worker/exception/JniCallException",
                     message);
}

}  // namespace pangrova::jni

namespace {
using pangrova::executor::TaskExecutor;

std::shared_ptr<TaskExecutor> lookupExecutor(JNIEnv* env, jlong handle) {
  try {
    return pangrova::jni::HandleRegistry::instance().acquire(
        static_cast<int64_t>(handle));
  } catch (const std::runtime_error& e) {
    pangrova::jni::throwJniCallException(env, e.what());
    return nullptr;
  }
}

std::vector<uint8_t> toVector(JNIEnv* env, jbyteArray array) {
  if (!array) return {};
  jsize len = env->GetArrayLength(array);
  if (len == 0) return {};
  std::vector<uint8_t> buf(static_cast<size_t>(len));

  // Use GetPrimitiveArrayCritical for potentially zero-copy access to the
  // Java byte array. The JVM may return a direct pointer to the array's
  // internal storage, avoiding the copy that GetByteArrayRegion always does.
  // CRITICAL REGION CONTRACT: no JNI calls or blocking operations between
  // Get and Release. We only perform a memcpy within the region.
  void* critical = env->GetPrimitiveArrayCritical(array, nullptr);
  if (critical) {
    std::memcpy(buf.data(), critical, static_cast<size_t>(len));
    env->ReleasePrimitiveArrayCritical(array, critical, JNI_ABORT);
  } else {
    // Fallback: GetPrimitiveArrayCritical can return nullptr if the JVM
    // cannot pin the array (e.g. during GC pressure). Clear any pending
    // exception before using the safe but always-copying fallback.
    if (env->ExceptionCheck()) {
      env->ExceptionClear();
    }
    std::cerr << "level=WARN event=critical_array_pin_failed"
              << " fallback=GetByteArrayRegion"
              << " array_length=" << len
              << std::endl;
    env->GetByteArrayRegion(
        array, 0, len, reinterpret_cast<jbyte*>(buf.data()));
  }
  return buf;
}

jbyteArray toJbyteArray(JNIEnv* env, const std::vector<uint8_t>& buf) {
  if (buf.empty()) return env->NewByteArray(0);
  jbyteArray result = env->NewByteArray(static_cast<jint>(buf.size()));
  if (!result) return nullptr;

  // Use GetPrimitiveArrayCritical for potentially zero-copy write access.
  // CRITICAL REGION CONTRACT: only memcpy, no JNI calls or blocking.
  void* critical = env->GetPrimitiveArrayCritical(result, nullptr);
  if (critical) {
    std::memcpy(critical, buf.data(), buf.size());
    // Mode 0: copy back and free the buffer (commit the write).
    env->ReleasePrimitiveArrayCritical(result, critical, 0);
  } else {
    // Fallback to safe copy path.
    env->SetByteArrayRegion(result, 0, static_cast<jint>(buf.size()),
                            reinterpret_cast<const jbyte*>(buf.data()));
  }
  return result;
}

std::string toString(JNIEnv* env, jstring jstr) {
  if (!jstr) return {};
  const char* chars = env->GetStringUTFChars(jstr, nullptr);
  if (!chars) {
    // GetStringUTFChars returns nullptr on allocation failure (OOM).
    // A pending OutOfMemoryError is already thrown on the Java side.
    return {};
  }
  std::string result(chars);
  env->ReleaseStringUTFChars(jstr, chars);
  return result;
}
}  // anonymous namespace

extern "C" {

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeInitInternal(
    JNIEnv* env, jclass, jbyteArray configProto, jobject allocationListener) {
  try {
    // Initialize JNI reference cache first — caches jclass (via NewGlobalRef)
    // and jmethodID for NativeRuntime, JniCallException, ArrowArray,
    // ArrowSchema. Must happen before any JNI callback that uses the cache.
    pangrova::jni::initJniReferenceCache(env);

    auto data = toVector(env, configProto);
    auto config = pangrova::jni::parseNativeConfig(data);
    // Runtime re-initialization invalidates process-local credentials. Stop all
    // data-plane listeners before atomically replacing their security context.
    pangrova::jni::shutdownFlightResultServer();
    pangrova::jni::FlightServerRegistry::instance().configureShuffleResources(config);
    pangrova::jni::FlightServerRegistry::instance().configureTransportSecurity(
        config.transport_tls_enabled,
        config.transport_tls_certificate_chain_path,
        config.transport_tls_private_key_path,
        config.transport_tls_root_ca_path,
        config.transport_tls_identity);
    pangrova::velox::PangrovaExchangeSource::configureTransportSecurity(
        config.transport_tls_enabled,
        config.transport_tls_certificate_chain_path,
        config.transport_tls_private_key_path,
        config.transport_tls_root_ca_path,
        config.transport_tls_identity);
    pangrova::jni::initializeVeloxRuntime(config, env, allocationListener);
  } JNI_CATCH_AND_THROW(env)
}

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeShutdownInternal(
    JNIEnv* env, jclass) {
  try {
    // Stop FlightResultServer BEFORE Velox shutdown — it depends on
    // Velox OutputBufferManager which is destroyed during Velox shutdown.
    pangrova::jni::shutdownFlightResultServer();
    // Release the GlobalRef for the Java NativeWriter before Velox shutdown.
    // Must happen while JNIEnv is still valid and before memory pools are destroyed.
    pangrova::jni::GlobalWriterRegistry::instance().clear(env);
    pangrova::jni::GlobalEgressRegistry::instance().clear(env);
    pangrova::jni::shutdownVeloxRuntime();
    pangrova::udf::clearJvmFunctionCallbacks(env);
    // Release cached JNI global references after Velox shutdown.
    // Must happen last — Velox shutdown may still use JNI callbacks.
    pangrova::jni::releaseJniReferenceCache(env);
  } JNI_CATCH_AND_THROW(env)
}

// P1-7: Triggers native memory pool reclaim from Java when the Java-side
// arbitrator denies an allocation. Calls shrinkCapacity with allowSpill=true
// to trigger operator MemoryReclaimer spill across managed pools.
JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeRequestReclaim(
    JNIEnv* env, jclass, jlong targetBytes) {
  pangrova::jni::JniLocalFrameGuard frame(env, 2);
  if (!frame.valid()) {
    return 0;
  }
  try {
    auto* governor = pangrova::jni::globalVeloxMemoryGovernor();
    if (!governor || !governor->veloxMemoryManager()) {
      return 0;
    }
    auto* arbitrator = governor->veloxMemoryManager()->arbitrator();
    if (!arbitrator) {
      return 0;
    }
    uint64_t freed = arbitrator->shrinkCapacity(
        static_cast<uint64_t>(targetBytes),
        /*allowSpill=*/true,
        /*allowAbort=*/false);
    return static_cast<jlong>(freed);
  } JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeRegisterFunctions0(
  JNIEnv* env, jclass, jobject callback, jobjectArray descriptors) {
  try {
    if (!callback || !descriptors) {
      throw std::invalid_argument(
          "Function callback and descriptors must not be null");
    }
    auto registration = pangrova::jni::NativeEngineLifecycle::instance()
        .functionCatalogStartup()->beginRegistration();
    auto* registry = pangrova::jni::globalUdfRegistry();
    if (!registry) {
      throw std::runtime_error(
          "Native UDF registry is unavailable before function registration");
    }

    std::vector<pangrova::udf::ScalarUdfSpec> scalarSpecs;
    const jsize count = env->GetArrayLength(descriptors);
    scalarSpecs.reserve(static_cast<size_t>(count));
    for (jsize i = 0; i < count; ++i) {
      auto bytes = static_cast<jbyteArray>(
          env->GetObjectArrayElement(descriptors, i));
      if (!bytes) {
        throw std::invalid_argument(
            "Function descriptor at index " + std::to_string(i) +
            " is null");
      }
      const auto serialized = toVector(env, bytes);
      env->DeleteLocalRef(bytes);
      pangrova::protocol::v2::NativeFunctionDescriptor descriptor;
      if (!descriptor.ParseFromArray(
              serialized.data(), static_cast<int>(serialized.size()))) {
        throw std::invalid_argument(
            "Function descriptor at index " + std::to_string(i) +
            " is malformed");
      }
      scalarSpecs.push_back(pangrova::udf::makeJvmScalarFunctionSpec(descriptor));
    }

    // Preflight the entire batch before retaining or publishing a callback.
    // Invalid/duplicate/finalized registration must leave the active bridge
    // and its factories untouched, not clear them in an error recovery path.
    registry->withScalars(scalarSpecs);
    const auto callbackOwner =
        pangrova::udf::prepareJvmFunctionCallbacks(env, callback, pangrova::jni::HandleRegistry::instance());
    for (auto& spec : scalarSpecs) {
      pangrova::udf::bindJvmFunctionCallbacks(spec, callbackOwner);
    }
    auto candidate = registry->withScalars(scalarSpecs);
    candidate.registerAllWithVelox();
    *registry = std::move(candidate);
    pangrova::udf::publishJvmFunctionCallbacks(callbackOwner);
    registration.complete();
  } JNI_CATCH_AND_THROW(env)
}

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCreateTaskExecutor0(
    JNIEnv* env, jclass, jbyteArray taskRequestProto) {
  pangrova::jni::JniLocalFrameGuard frame(env, 4);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(
        env, "PushLocalFrame failed in nativeCreateTaskExecutor");
    return 0;
  }
  try {
    return pangrova::jni::createTaskExecutor(env, pangrova::jni::parseTaskRequest(toVector(env, taskRequestProto)));
  } JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCreateTaskExecutorWithTrace0(
    JNIEnv* env, jclass, jbyteArray taskRequestProto, jstring traceId) {
  // PushLocalFrame: toVector creates jbyteArray region access,
  // toString creates local refs from GetStringUTFChars.
  pangrova::jni::JniLocalFrameGuard frame(env, 8);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(
        env, "PushLocalFrame failed in nativeCreateTaskExecutorWithTrace");
    return 0;
  }
  try {
    return pangrova::jni::createTaskExecutor(env, pangrova::jni::parseTaskRequest(toVector(env, taskRequestProto)),
                                           toString(env, traceId));
  } JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCreateTaskExecutorWithTraceContext0(
    JNIEnv* env, jclass, jbyteArray taskRequestProto, jstring traceId, jstring spanId) {
  pangrova::jni::JniLocalFrameGuard frame(env, 8);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(
        env, "PushLocalFrame failed in nativeCreateTaskExecutorWithTraceContext");
    return 0;
  }
  try {
    return pangrova::jni::createTaskExecutor(env, pangrova::jni::parseTaskRequest(toVector(env, taskRequestProto)),
                                           toString(env, traceId), toString(env, spanId));
  } JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeStartTask0(
    JNIEnv* env, jclass, jlong executorHandle) {
  try {
    auto executor = lookupExecutor(env, executorHandle);
    if (executor) executor->start();
  } JNI_CATCH_AND_THROW(env)
}

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCancelTask0(
    JNIEnv* env, jclass, jlong executorHandle) {
  try {
    auto executor = lookupExecutor(env, executorHandle);
    if (executor) executor->cancel();
  } JNI_CATCH_AND_THROW(env)
}

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeReleaseExecutor0(
    JNIEnv* env, jclass, jlong executorHandle) {
  try {
    // TaskExecutor's actual shuffle owner seals START and quiesces its borrowed
    // queue before driver/output release. The receipt follows that same exit.
    jbyteArray receipt = nullptr;
    pangrova::jni::HandleRegistry::instance().release(
        executorHandle, [&](pangrova::executor::TaskExecutor& executor) {
          const auto bytes = pangrova::jni::serializeTaskStatus(executor);
          if (bytes.empty() || bytes.size() > static_cast<size_t>(std::numeric_limits<jsize>::max())) {
            throw std::runtime_error("Released task report cannot be represented by a JNI array");
          }
          receipt = env->NewByteArray(static_cast<jsize>(bytes.size()));
          if (!receipt || env->ExceptionCheck()) {
            throw std::runtime_error("Failed to allocate the released task report");
          }
          env->SetByteArrayRegion(receipt, 0, static_cast<jsize>(bytes.size()),
                                 reinterpret_cast<const jbyte*>(bytes.data()));
          if (env->ExceptionCheck()) {
            throw std::runtime_error("Failed to copy the released task report");
          }
        });
    return receipt;
  } catch (const std::exception& failure) {
    if (!env->ExceptionCheck()) pangrova::jni::throwJniCallException(env, failure.what());
  } catch (...) {
    if (!env->ExceptionCheck()) pangrova::jni::throwJniCallException(env, "Unknown release error");
  }
  return nullptr;
}

// nativeGetNextBatch — REMOVED: data path migrated to C++ FlightResultServer.
// nativeGetNextBatchColumnar — REMOVED: data path migrated to C++ FlightResultServer.
// nativeGetShuffleBatch — REMOVED: shuffle path migrated to native FlightShuffleServer.

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeWatchResultDrain0(
    JNIEnv* env, jclass, jlong executorHandle, jobject callback) {
  try {
    auto executor = lookupExecutor(env, executorHandle);
    if (!executor) return;
    auto listener = std::make_shared<pangrova::jni::ResultDrainUpcall>(env, callback, executorHandle);
    executor->watchResultDrain([listener = std::move(listener)] { listener->notify(); });
  } JNI_CATCH_AND_THROW(env)
}

JNIEXPORT jint JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeReadTaskState0(
    JNIEnv* env, jclass, jlong executorHandle, jint waitMillis) {
  try {
    if (waitMillis < 0) throw std::invalid_argument("Task state wait must be non-negative");
    const auto executor = lookupExecutor(env, executorHandle);
    if (executor) {
      const auto state = waitMillis == 0
          ? executor->state()
          : executor->awaitTerminal(std::chrono::milliseconds(waitMillis));
      return static_cast<jint>(pangrova::jni::toProtoTaskState(state));
    }
  } JNI_CATCH_AND_THROW(env)
  return pangrova::protocol::v2::TASK_STATE_UNSPECIFIED;
}

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetTaskStatus0(
    JNIEnv* env, jclass, jlong executorHandle) {
  pangrova::jni::JniLocalFrameGuard frame(env, 4);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(
        env, "PushLocalFrame failed in nativeGetTaskStatus");
    return nullptr;
  }
  try {
    auto executor = lookupExecutor(env, executorHandle);
    if (!executor) return nullptr;
    // removeTerminated() was previously called here every 16th poll as
    // opportunistic GC for leaked handles. Removed: it races with Flight
    // DoGet for result tasks — destroying a finished executor's Velox Task
    // also removes its OutputBuffer from OutputBufferManager, causing
    // "taskId not registered" errors when the client fetches results.
    // The Java side manages executor lifecycle via nativeReleaseExecutor;
    // removeTerminated() is only needed as a safety net and is still
    // available for explicit invocation if needed.
    jbyteArray result =
        toJbyteArray(env, pangrova::jni::serializeTaskStatus(*executor));
    return static_cast<jbyteArray>(frame.popWithResult(result));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetTaskStats0(
    JNIEnv* env, jclass, jlong executorHandle) {
  pangrova::jni::JniLocalFrameGuard frame(env, 4);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(
        env, "PushLocalFrame failed in nativeGetTaskStats");
    return nullptr;
  }
  try {
    auto executor = lookupExecutor(env, executorHandle);
    if (!executor) return nullptr;
    jbyteArray result =
        toJbyteArray(env, pangrova::jni::serializeTaskStats(*executor));
    return static_cast<jbyteArray>(frame.popWithResult(result));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

// nativeAddExchangeBatch — REMOVED: exchange path migrated to native PangrovaExchangeSource.
// nativeNoMoreExchangeData — REMOVED: exchange path migrated to native PangrovaExchangeSource.
// nativeNoMoreExchangeSources — REMOVED: exchange path migrated to native PangrovaExchangeSource.

// nativeInjectDynamicFilter — REMOVED: migrated to unified command channel.
// nativeUpdateMemoryLimit — REMOVED: migrated to unified command channel.

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeStartTaskWithTrace0(
    JNIEnv* env, jclass, jlong executorHandle, jstring traceId) {
  pangrova::jni::JniLocalFrameGuard frame(env, 4);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(
        env, "PushLocalFrame failed in nativeStartTaskWithTrace");
    return;
  }
  try {
    auto executor = lookupExecutor(env, executorHandle);
    if (!executor) return;
    std::string tid = toString(env, traceId);
    if (!tid.empty()) {
      executor->setTraceId(tid);
    }
    executor->start();
  } JNI_CATCH_AND_THROW(env)
}

// nativeGetCacheStats — REMOVED: migrated to unified command channel
// nativePrefetchSplits — REMOVED: migrated to unified command channel
// nativeEvictCache — REMOVED: migrated to unified command channel

// nativeDecodeBatchToPointers — REMOVED: exchange IPC fallback path no longer used.

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCommand0(
    JNIEnv* env, jclass, jlong executorHandle, jbyteArray commandProto) {
  // P0-BUG-4: Use the JNI executorHandle parameter as the authoritative handle.
  // The proto payload also carries executor_handle (field 1), but the JNI
  // parameter takes precedence to prevent routing mismatches if the two
  // values ever diverge. We patch the parsed NativeCommand before dispatch.
  pangrova::jni::JniLocalFrameGuard frame(env, 8);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(
        env, "PushLocalFrame failed in nativeCommand");
    return nullptr;
  }
  try {
    auto data = toVector(env, commandProto);
    auto result = pangrova::jni::dispatchCommandWithHandle(
        data, static_cast<int64_t>(executorHandle));
    jbyteArray out = toJbyteArray(env, result);
    return static_cast<jbyteArray>(frame.popWithResult(out));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativePushExchangeChunk0(
    JNIEnv* env, jclass, jlong executorHandle, jbyteArray metadata, jbyteArray data) {
  pangrova::jni::JniLocalFrameGuard frame(env, 8);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(env, "PushLocalFrame failed in nativePushExchangeChunk");
    return nullptr;
  }
  try {
    if (!metadata || !data) throw std::invalid_argument("Exchange chunk requires metadata and data");
    const jsize length = env->GetArrayLength(data);
    if (length <= 0 ||
        static_cast<size_t>(length) > pangrova::shuffle::kCanonicalShuffleChunkBytes) {
      throw std::invalid_argument("Exchange chunk exceeds canonical payload bounds");
    }
    std::string bytes(static_cast<size_t>(length), '\0');
    env->GetByteArrayRegion(data, 0, length, reinterpret_cast<jbyte*>(bytes.data()));
    if (env->ExceptionCheck()) return nullptr;
    auto result = pangrova::jni::dispatchExchangeChunk(
        toVector(env, metadata), static_cast<int64_t>(executorHandle), std::move(bytes));
    return static_cast<jbyteArray>(frame.popWithResult(toJbyteArray(env, result)));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetMemorySnapshot0(
    JNIEnv* env, jclass) {
  pangrova::jni::JniLocalFrameGuard frame(env, 4);
  if (!frame.valid()) {
    return nullptr;
  }
  try {
    auto* bridge = pangrova::jni::globalVeloxMemoryGovernor();
    if (!bridge) {
      return nullptr;
    }
    auto snapshot = bridge->getMemorySnapshot();
    // P1-JNI-6: Proto is always available — no conditional guard needed.
    pangrova::protocol::v2::MemorySnapshot proto;
    proto.set_current_used_bytes(snapshot.current_used_bytes);
    proto.set_peak_used_bytes(snapshot.peak_used_bytes);
    proto.set_spill_bytes(snapshot.spill_bytes);
    proto.set_cache_used_bytes(snapshot.cache_used_bytes);
    proto.set_arbitrator_free_bytes(snapshot.arbitrator_free_bytes);
    proto.set_arbitrator_capacity_bytes(snapshot.arbitrator_capacity_bytes);
    proto.set_active_pool_count(snapshot.active_pool_count);
    std::vector<uint8_t> bytes(proto.ByteSizeLong());
    proto.SerializeToArray(bytes.data(), static_cast<int>(bytes.size()));
    jbyteArray out = toJbyteArray(env, bytes);
    return static_cast<jbyteArray>(frame.popWithResult(out));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

// PERF-T3: Thread-local cached IPC writer/sink to avoid per-call heap allocation.
struct TlsIpcCache {
  std::shared_ptr<arrow::io::BufferOutputStream> sink;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  std::shared_ptr<arrow::Schema> schema;
};

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeSerializeBatchFromPointers0(
    JNIEnv* env, jclass, jlong arrowArrayPtr, jlong arrowSchemaPtr) {
  pangrova::jni::JniLocalFrameGuard frame(env, 4);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(
        env, "PushLocalFrame failed in nativeSerializeBatchFromPointers");
    return nullptr;
  }
  try {
    if (arrowArrayPtr == 0 || arrowSchemaPtr == 0) {
      pangrova::jni::throwJniCallException(
          env, "nativeSerializeBatchFromPointers: null ArrowArray or ArrowSchema pointer");
      return nullptr;
    }
    auto* array = reinterpret_cast<ArrowArray*>(arrowArrayPtr);
    auto* schema = reinterpret_cast<ArrowSchema*>(arrowSchemaPtr);

    auto import_result = pangrova::jni::ColumnarDataExporter::importBatchFromJava(
        array, schema);
    if (!import_result.ok()) {
      throw std::runtime_error(
          "nativeSerializeBatchFromPointers: import failed: " +
          import_result.status().ToString());
    }
    auto batch = *import_result;

    // PERF-T3: Reuse thread_local sink and writer to eliminate per-call
    // BufferOutputStream::Create() and MakeStreamWriter() overhead.
    static thread_local TlsIpcCache tls_ipc;

    auto batch_schema = batch->schema();
    bool schema_changed = !tls_ipc.schema || !tls_ipc.schema->Equals(*batch_schema);

    // Always create a fresh sink (BufferOutputStream::Finish() consumes it)
    auto sink_result = arrow::io::BufferOutputStream::Create();
    if (!sink_result.ok()) {
      throw std::runtime_error(
          "nativeSerializeBatchFromPointers: buffer creation failed");
    }
    tls_ipc.sink = *sink_result;

    if (schema_changed || !tls_ipc.writer) {
      // Schema changed or first call — rebuild writer
      auto writer_result = arrow::ipc::MakeStreamWriter(tls_ipc.sink, batch_schema);
      if (!writer_result.ok()) {
        throw std::runtime_error(
            "nativeSerializeBatchFromPointers: writer creation failed: " +
            writer_result.status().ToString());
      }
      tls_ipc.writer = *writer_result;
      tls_ipc.schema = batch_schema;
    } else {
      // Same schema — create writer reusing cached schema (avoids re-parsing)
      auto writer_result = arrow::ipc::MakeStreamWriter(tls_ipc.sink, tls_ipc.schema);
      if (!writer_result.ok()) {
        throw std::runtime_error(
            "nativeSerializeBatchFromPointers: writer creation failed: " +
            writer_result.status().ToString());
      }
      tls_ipc.writer = *writer_result;
    }

    auto write_status = tls_ipc.writer->WriteRecordBatch(*batch);
    if (!write_status.ok()) {
      throw std::runtime_error(
          "nativeSerializeBatchFromPointers: write failed: " +
          write_status.ToString());
    }

    auto close_status = tls_ipc.writer->Close();
    if (!close_status.ok()) {
      throw std::runtime_error(
          "nativeSerializeBatchFromPointers: close failed: " +
          close_status.ToString());
    }

    auto finish_result = tls_ipc.sink->Finish();
    if (!finish_result.ok()) {
      throw std::runtime_error(
          "nativeSerializeBatchFromPointers: finish failed: " +
          finish_result.status().ToString());
    }
    auto buffer = *finish_result;

    jbyteArray result = env->NewByteArray(static_cast<jint>(buffer->size()));
    if (!result) return nullptr;
    if (buffer->size() > 0) {
      env->SetByteArrayRegion(result, 0, static_cast<jint>(buffer->size()),
                              reinterpret_cast<const jbyte*>(buffer->data()));
    }
    return static_cast<jbyteArray>(frame.popWithResult(result));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

JNIEXPORT jint JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetBatchRowCount0(
    JNIEnv* env, jclass, jlong arrowArrayPtr) {
  try {
    if (arrowArrayPtr == 0) {
      pangrova::jni::throwJniCallException(
          env, "nativeGetBatchRowCount: null ArrowArray pointer");
      return 0;
    }
    auto* array = reinterpret_cast<const ArrowArray*>(arrowArrayPtr);
    return static_cast<jint>(array->length);
  } JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT jint JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetBatchColumnCount0(
    JNIEnv* env, jclass, jlong arrowSchemaPtr) {
  try {
    if (arrowSchemaPtr == 0) {
      pangrova::jni::throwJniCallException(
          env, "nativeGetBatchColumnCount: null ArrowSchema pointer");
      return 0;
    }
    auto* schema = reinterpret_cast<const ArrowSchema*>(arrowSchemaPtr);
    return static_cast<jint>(schema->n_children);
  } JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeRegisterWriter0(
    JNIEnv* env, jclass, jobject writer) {
  try {
    if (!writer) {
      pangrova::jni::throwJniCallException(
          env, "nativeRegisterWriter: writer must not be null");
      return;
    }
    pangrova::jni::GlobalWriterRegistry::instance().registerDefaultWriter(env, writer);
  } JNI_CATCH_AND_THROW(env)
}

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeSetHmacKey0(
    JNIEnv* env, jclass, jbyteArray currentKey, jbyteArray previousKey) {
  try {
    if (!currentKey) {
      pangrova::jni::throwJniCallException(
          env, "nativeSetHmacKey: currentKey must not be null");
      return;
    }
    auto currentKeyBytes = toVector(env, currentKey);
    auto previousKeyBytes = toVector(env, previousKey);

    std::string currentKeyStr(
        reinterpret_cast<const char*>(currentKeyBytes.data()),
        currentKeyBytes.size());
    std::string previousKeyStr(
        reinterpret_cast<const char*>(previousKeyBytes.data()),
        previousKeyBytes.size());

    // Store the HMAC key in FlightServerRegistry so that the shuffle server
    // uses it when created or recreated. The registry passes the key to
    // FlightShuffleServerConfig.hmac_current_key / hmac_previous_key.
    pangrova::jni::FlightServerRegistry::instance().setHmacKey(
        currentKeyStr, previousKeyStr);
    pangrova::shuffle::ShuffleTicketKeys::instance().set(
        std::move(currentKeyStr), std::move(previousKeyStr));
  } JNI_CATCH_AND_THROW(env)
}

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeRegisterEgress0(
    JNIEnv* env, jclass, jobject egress) {
  try {
    if (!egress) {
      pangrova::jni::throwJniCallException(
          env, "nativeRegisterEgress: egress must not be null");
      return;
    }
    pangrova::jni::GlobalEgressRegistry::instance().registerDefaultEgress(env, egress);
  } JNI_CATCH_AND_THROW(env)
}

}  // extern "C"

// ---------------------------------------------------------------------------
// BM25 Native Scorer JNI — batch BM25 scoring for full-text hybrid search.
// ---------------------------------------------------------------------------
#include "indexing/text/Bm25Scorer.h"

extern "C" {

JNIEXPORT jdoubleArray JNICALL
Java_org_pangrova_ai_worker_text_Bm25NativeScorer_nativeScoreBatch(
    JNIEnv* env, jclass,
    jdoubleArray idfs, jintArray termFreqs, jintArray docLengths,
    jint numDocs, jint numTerms, jdouble k1, jdouble b, jdouble avgDocLength) {
  pangrova::jni::JniLocalFrameGuard frame(env, 8);
  if (!frame.valid()) {
    pangrova::jni::throwJniCallException(
        env, "PushLocalFrame failed in nativeScoreBatch");
    return nullptr;
  }
  try {
    if (!idfs || !termFreqs || !docLengths) {
      pangrova::jni::throwJniCallException(
          env, "nativeScoreBatch: null array argument");
      return nullptr;
    }

    pangrova::text::Bm25Config config;
    config.k1 = static_cast<double>(k1);
    config.b = static_cast<double>(b);
    config.avgDocLength = static_cast<double>(avgDocLength);

    // PERF-T6: Use GetPrimitiveArrayCritical for zero-copy JNI array access.
    // WARNING: No other JNI calls allowed while critical arrays are held.
    jdouble* idfPtr = (jdouble*)env->GetPrimitiveArrayCritical(idfs, nullptr);
    jint* tfPtr = (jint*)env->GetPrimitiveArrayCritical(termFreqs, nullptr);
    jint* dlPtr = (jint*)env->GetPrimitiveArrayCritical(docLengths, nullptr);

    if (!idfPtr || !tfPtr || !dlPtr) {
      if (idfPtr) env->ReleasePrimitiveArrayCritical(idfs, idfPtr, JNI_ABORT);
      if (tfPtr) env->ReleasePrimitiveArrayCritical(termFreqs, tfPtr, JNI_ABORT);
      if (dlPtr) env->ReleasePrimitiveArrayCritical(docLengths, dlPtr, JNI_ABORT);
      pangrova::jni::throwJniCallException(
          env, "nativeScoreBatch: GetPrimitiveArrayCritical returned null");
      return nullptr;
    }

    // PERF-T6: Inlined BM25 scoring with loop structure friendly to
    // compiler auto-vectorization (no branches, no function calls in inner loop).
    int nd = static_cast<int>(numDocs);
    int nt = static_cast<int>(numTerms);
    std::vector<double> scores(nd, 0.0);

    const double k1_val = config.k1;
    const double b_val = config.b;
    const double avgDl = config.avgDocLength;
    const double k1_plus_1 = k1_val + 1.0;
    const double b_div_avg = b_val / avgDl;
    const double one_minus_b = 1.0 - b_val;

    for (int t = 0; t < nt; ++t) {
      double termIdf = idfPtr[t];
      const jint* tfRow = tfPtr + t * nd;
      // Inner loop: auto-vectorizable (no branches, simple arithmetic)
      for (int d = 0; d < nd; ++d) {
        double tf = static_cast<double>(tfRow[d]);
        double dl = static_cast<double>(dlPtr[d]);
        double norm = one_minus_b + b_div_avg * dl;
        scores[d] += termIdf * (tf * k1_plus_1) / (tf + k1_val * norm);
      }
    }

    // Release with JNI_ABORT (read-only, skip copy-back)
    env->ReleasePrimitiveArrayCritical(docLengths, dlPtr, JNI_ABORT);
    env->ReleasePrimitiveArrayCritical(termFreqs, tfPtr, JNI_ABORT);
    env->ReleasePrimitiveArrayCritical(idfs, idfPtr, JNI_ABORT);

    jdoubleArray result = env->NewDoubleArray(nd);
    if (!result) return nullptr;
    env->SetDoubleArrayRegion(result, 0, nd, scores.data());
    return static_cast<jdoubleArray>(frame.popWithResult(result));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

}  // extern "C"

// ---------------------------------------------------------------------------
// JNI_OnLoad — called once by the JVM when the native library is loaded.
// ---------------------------------------------------------------------------
extern "C" {

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* /*reserved*/) {
  pangrova::jni::cacheJavaVM(vm);
  return JNI_VERSION_1_8;
}

}  // extern "C" (JNI_OnLoad)
