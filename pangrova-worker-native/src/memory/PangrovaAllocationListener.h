// PangrovaAllocationListener: JNI callback layer for memory arbitration.
//
// Delegates memory allocation requests to Java-side AllocationListener
// via synchronous JNI calls within the same Java-initiated call chain.
//
// Thread safety: must only be called from threads already attached to JVM
// (Java-initiated JNI call chains). Does NOT independently AttachCurrentThread.
//
// GlobalRef lifecycle: created in constructor, deleted in destructor via RAII.
// Exception path safety guaranteed by destructor + RAII guard pattern.
//
// Placement: memory/ — memory coordination components.
#pragma once

#include <jni.h>
#include <atomic>
#include <cstdint>

namespace pangrova::memory {

class PangrovaArbitrator;  // forward decl — see PangrovaArbitrator.h



// RAII guard for JNI GlobalRef lifecycle management.
// Ensures GlobalRef is deleted even on exception paths.
class GlobalRefGuard {
 public:
  GlobalRefGuard(JavaVM* vm, jobject ref) : vm_(vm), ref_(ref) {}

  ~GlobalRefGuard() {
    release();
  }

  // Non-copyable (unique ownership of GlobalRef).
  GlobalRefGuard(const GlobalRefGuard&) = delete;
  GlobalRefGuard& operator=(const GlobalRefGuard&) = delete;

  // Move-constructible and move-assignable (transfers ownership).
  GlobalRefGuard(GlobalRefGuard&& other) noexcept
      : vm_(other.vm_), ref_(other.ref_) {
    other.vm_ = nullptr;
    other.ref_ = nullptr;
  }

  GlobalRefGuard& operator=(GlobalRefGuard&& other) noexcept {
    if (this != &other) {
      release();
      vm_ = other.vm_;
      ref_ = other.ref_;
      other.vm_ = nullptr;
      other.ref_ = nullptr;
    }
    return *this;
  }

  // Release the GlobalRef. Safe to call multiple times (idempotent).
  void release() {
    if (ref_ == nullptr) {
      return;
    }
    JNIEnv* env = nullptr;
    if (vm_ && vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) == JNI_OK) {
      env->DeleteGlobalRef(ref_);
    }
    ref_ = nullptr;
  }

  // Access the underlying GlobalRef (non-owning view).
  jobject get() const { return ref_; }

 private:
  JavaVM* vm_;
  jobject ref_;
};

// Synchronous JNI callback listener for Velox memory arbitration.
//
// Holds a GlobalRef to Java AllocationListener, invokes reserve()/unreserve()
// on the same Java thread that initiated the JNI call chain.
//
// @NotThreadSafe — must be called from a single arbitration thread
// (the Java thread that initiated the JNI call).
class PangrovaAllocationListener {
 public:
  // Construct from a JNI-attached thread.
  // env: valid JNIEnv from the calling Java thread.
  // javaListener: Java AllocationListener object (GlobalRef will be created).
  //
  // Precondition: current thread is attached to JVM.
  // Throws std::runtime_error if GlobalRef creation or method lookup fails.
  PangrovaAllocationListener(JNIEnv* env, jobject javaListener,
                          int64_t non_jvm_cap_bytes = 0);

  ~PangrovaAllocationListener();

  // Non-copyable, non-movable (owns GlobalRef).
  PangrovaAllocationListener(const PangrovaAllocationListener&) = delete;
  PangrovaAllocationListener& operator=(const PangrovaAllocationListener&) = delete;
  PangrovaAllocationListener(PangrovaAllocationListener&&) = delete;
  PangrovaAllocationListener& operator=(PangrovaAllocationListener&&) = delete;

  // Notify Java side of allocation change.
  //
  // diff > 0: reserve bytes from Java side.
  //   Returns actual granted bytes (0 = denied, caller should trigger spill).
  // diff < 0: unreserve |diff| bytes to Java side.
  //   Returns |diff| (always succeeds for release).
  // diff == 0: no-op, returns 0.
  //
  // Precondition: current thread is attached to JVM (VELOX_CHECK enforced).
  // JNI exceptions from Java side are checked and converted to return 0.
  int64_t allocationChanged(int64_t diff);

  // Task 27.7: install/clear the PangrovaArbitrator-managed non-JVM root pool.
  //
  // When set, the env==nullptr path delegates reserve/release to the
  // arbitrator's non-JVM root pool, replacing this listener's internal
  // CAS-drain counter. Pass nullptr to disconnect (must be done before the
  // arbitrator is destroyed to avoid dangling delegation).
  // Idempotent and lock-free (atomic pointer store).
  void setNonJvmRootPoolDelegate(PangrovaArbitrator* arbitrator);

 private:
  // Obtain JNIEnv for the current thread.
  // Returns nullptr if the thread is not JVM-attached (e.g. Velox I/O threads).
  JNIEnv* getEnv() const;

  JavaVM* jvm_;
  GlobalRefGuard listener_guard_;  // RAII guard for GlobalRef lifecycle
  jmethodID reserve_method_;       // AllocationListener.reserve(long) -> long
  jmethodID unreserve_method_;     // AllocationListener.unreserve(long) -> void

  // P0-BUG-4: Track non-JVM allocations (from Velox I/O threads that bypass
  // Java-side arbitration). Exposed via nonJvmAllocatedBytes() for memory
  // snapshot reporting. Used only when non_jvm_root_pool_ == nullptr (the
  // fallback path); when an PangrovaArbitrator-managed root pool is installed
  // via setNonJvmRootPoolDelegate(), this counter stays at zero.
  std::atomic<int64_t> non_jvm_allocated_bytes_{0};

  // P3-BUG-18: Configurable cap for non-JVM thread allocations.
  // Default 512MB when not explicitly configured.
  static constexpr int64_t kDefaultNonJvmCapBytes = 512L * 1024 * 1024;
  int64_t non_jvm_cap_bytes_;

  // Task 27.7: optional delegation target for non-JVM allocations. When
  // non-null, the env==nullptr path of allocationChanged() routes reserve
  // and release through the arbitrator's non-JVM root pool instead of the
  // local CAS-drain counter. Atomic pointer for lock-free hot-path read.
  std::atomic<PangrovaArbitrator*> non_jvm_root_pool_{nullptr};

  // Cross-thread unreserve debt: when memory allocated via Java reserve (JVM
  // thread) is freed on a non-JVM thread (folly executor), the non-JVM thread
  // cannot call Java unreserve. The freed amount accumulates here and is
  // drained on the next JVM-thread allocationChanged() call.
  std::atomic<int64_t> cross_thread_unreserve_debt_{0};

  // A-MEM-02: cumulative bytes of cross-thread unreserve debt that could NOT be drained to Java
  // (the unreserve JNI call failed, e.g. a torn-down/detached JVM) and were intentionally DROPPED
  // rather than re-accumulated. Re-accumulating under persistent failure grows the debt unboundedly
  // and self-locks memory accounting (native OOM). The bytes are already physically freed; this
  // counter surfaces the resulting Java-side over-count as an observable, bounded drift.
  std::atomic<int64_t> cross_thread_unreserve_drift_bytes_{0};

 public:
  // Current bytes allocated by non-JVM threads (bypassing Java arbitration).
  int64_t nonJvmAllocatedBytes() const {
    return non_jvm_allocated_bytes_.load(std::memory_order_relaxed);
  }

  // A-MEM-02: cumulative cross-thread unreserve debt that failed to drain and was dropped
  // (accounting drift, not a leak). Exposed for observability / drift monitoring.
  int64_t crossThreadUnreserveDriftBytes() const {
    return cross_thread_unreserve_drift_bytes_.load(std::memory_order_relaxed);
  }
};

}  // namespace pangrova::memory
