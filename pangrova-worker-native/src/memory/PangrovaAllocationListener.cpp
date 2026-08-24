// PangrovaAllocationListener: JNI callback layer for memory arbitration.
// See PangrovaAllocationListener.h for design rationale and thread safety contract.
#include "memory/PangrovaAllocationListener.h"

#include <stdexcept>

#include "jni/bridge/JniExceptionScope.h"
#include "memory/PangrovaArbitrator.h"
#include "observability/PangrovaLog.h"
#include "velox/common/base/Exceptions.h"

namespace pangrova::memory {

namespace {

// Java class name for AllocationListener interface.
constexpr const char* kAllocationListenerClass =
    "org/pangrova/worker/memory/AllocationListener";

}  // namespace

PangrovaAllocationListener::PangrovaAllocationListener(
    JNIEnv* env,
    jobject javaListener,
    int64_t non_jvm_cap_bytes)
    : jvm_(nullptr),
      listener_guard_(nullptr, nullptr),
      reserve_method_(nullptr),
      unreserve_method_(nullptr),
      non_jvm_cap_bytes_(non_jvm_cap_bytes > 0 ? non_jvm_cap_bytes : kDefaultNonJvmCapBytes) {
  VELOX_CHECK_NOT_NULL(env, "JNIEnv must not be null");
  VELOX_CHECK_NOT_NULL(javaListener, "Java AllocationListener must not be null");

  // Cache JavaVM pointer for later GetEnv calls.
  jint rc = env->GetJavaVM(&jvm_);
  VELOX_CHECK_EQ(rc, JNI_OK, "Failed to obtain JavaVM pointer");

  // Create GlobalRef to prevent Java GC from collecting the listener.
  jobject globalRef = env->NewGlobalRef(javaListener);
  VELOX_CHECK_NOT_NULL(
      globalRef, "Failed to create GlobalRef for AllocationListener");

  // Transfer ownership to RAII guard immediately.
  listener_guard_ = GlobalRefGuard(jvm_, globalRef);

  // Look up AllocationListener class and method IDs.
  jclass listenerClass = env->FindClass(kAllocationListenerClass);
  if (listenerClass == nullptr) {
    // FindClass failed — clear pending exception before throwing C++.
    env->ExceptionClear();
    VELOX_FAIL(
        "Failed to find Java class: {}", kAllocationListenerClass);
  }

  // reserve(long) -> long
  reserve_method_ = env->GetMethodID(listenerClass, "reserve", "(J)J");
  if (reserve_method_ == nullptr) {
    env->ExceptionClear();
    VELOX_FAIL("Failed to find method: AllocationListener.reserve(long)");
  }

  // unreserve(long) -> void
  unreserve_method_ = env->GetMethodID(listenerClass, "unreserve", "(J)V");
  if (unreserve_method_ == nullptr) {
    env->ExceptionClear();
    VELOX_FAIL("Failed to find method: AllocationListener.unreserve(long)");
  }

  // Clean up local reference to class (not needed after method ID lookup).
  env->DeleteLocalRef(listenerClass);

  PANGROVA_LOG(INFO) << "event=pangrova_allocation_listener_created";
}

PangrovaAllocationListener::~PangrovaAllocationListener() {
  // GlobalRefGuard destructor handles DeleteGlobalRef via RAII.
  PANGROVA_LOG(INFO) << "event=pangrova_allocation_listener_destroyed";
}

int64_t PangrovaAllocationListener::allocationChanged(int64_t diff) {
  if (diff == 0) {
    return 0;
  }

  JNIEnv* env = getEnv();

  // If the current thread is not JVM-attached (e.g. Velox internal I/O thread),
  // grant the allocation without Java-side arbitration. This is safe because:
  // 1. The JNI model forbids AttachCurrentThread from native side.
  // 2. Velox I/O threads are short-lived and bounded by the thread pool.
  // 3. The Java-side budget is still enforced for all JNI-initiated threads.
  //
  // Task 27.7: when an PangrovaArbitrator-managed non-JVM root pool is
  // installed, delegate reserve/release to it instead of using the local
  // CAS-drain counter. This places non-JVM allocations under the same
  // arbitrator that already governs JVM-driven pools, eliminating the
  // hardcoded 512MB cap and the CAS-drain correctness gap described in
  // PangrovaArbitrator.h's "non-JVM root pool" comment.
  // P0-BUG-4 / P0-VELOX-3: fallback path retains the local counter + cap
  // for unit tests and bootstrap paths that don't initialize an arbitrator.
  if (env == nullptr) {
    PangrovaArbitrator* delegate =
        non_jvm_root_pool_.load(std::memory_order_acquire);

    if (diff > 0) {
      // Reserve path — try delegate, then legacy counter.
      if (delegate != nullptr) {
        return delegate->reserveNonJvm(diff);
      }
      while (true) {
        int64_t current = non_jvm_allocated_bytes_.load(std::memory_order_relaxed);
        if (current + diff > non_jvm_cap_bytes_) {
          PANGROVA_LOG(WARNING) << "event=non_jvm_allocation_cap_exceeded"
                    << " current_bytes=" << current
                    << " requested_bytes=" << diff
                    << " cap_bytes=" << non_jvm_cap_bytes_;
          return 0;
        }
        if (non_jvm_allocated_bytes_.compare_exchange_weak(
                current, current + diff, std::memory_order_relaxed)) {
          return diff;
        }
      }
    }

    // Free path (diff < 0). Memory may have been allocated on a JVM thread
    // (via Java reserve). Try non-JVM accounting first; any remainder that
    // can't be attributed to non-JVM paths was Java-reserved, so accumulate
    // it as cross-thread debt for the next JVM-thread call to drain.
    int64_t releaseBytes = -diff;
    int64_t drained = 0;

    if (delegate != nullptr) {
      drained += delegate->releaseNonJvm(releaseBytes);
    }

    if (drained < releaseBytes) {
      while (true) {
        int64_t current = non_jvm_allocated_bytes_.load(std::memory_order_relaxed);
        if (current <= 0) break;
        int64_t drain = std::min(releaseBytes - drained, current);
        if (drain <= 0) break;
        if (non_jvm_allocated_bytes_.compare_exchange_weak(
                current, current - drain, std::memory_order_relaxed)) {
          drained += drain;
          break;
        }
      }
    }

    int64_t remainder = releaseBytes - drained;
    if (remainder > 0) {
      cross_thread_unreserve_debt_.fetch_add(remainder, std::memory_order_relaxed);
    }
    return releaseBytes;
  }

  // Drain cross-thread unreserve debt accumulated by non-JVM threads that
  // freed Java-reserved memory. This must happen before processing the
  // current diff so the Java budget has room for new allocations.
  int64_t debt = cross_thread_unreserve_debt_.exchange(0, std::memory_order_relaxed);
  if (debt > 0) {
    pangrova::jni::JniExceptionScope debtScope(
        env, "AllocationListener.unreserve(debt)",
        pangrova::jni::JniExceptionScope::Mode::kLog);
    env->CallVoidMethod(
        listener_guard_.get(), unreserve_method_, static_cast<jlong>(debt));
    if (debtScope.hasPending()) {
      // A-MEM-02: do NOT re-accumulate on drain failure. Under a persistently failing unreserve
      // (e.g. a torn-down/detached JVM) re-accumulation grows the debt unboundedly and self-locks
      // memory accounting (native OOM). The bytes were already physically freed on the native side,
      // so not re-draining only makes the Java budget over-count by 'debt' — a bounded, observable
      // DRIFT, not a leak. Record it on the drift counter instead of re-queuing an unbounded retry.
      int64_t cumulativeDrift =
          cross_thread_unreserve_drift_bytes_.fetch_add(debt, std::memory_order_relaxed) + debt;
      PANGROVA_LOG(WARNING) << "event=cross_thread_debt_drain_failed_dropped"
                << " debt_bytes=" << debt
                << " cumulative_drift_bytes=" << cumulativeDrift
                << " note=not_re_accumulated_to_avoid_self_lock";
    }
  }

  // BUG-FIX: Memory may be allocated on a non-JVM thread (env==nullptr)
  // but freed on a JVM thread (env!=nullptr). Two accounting paths exist:
  //   (a) delegate path: reserveNonJvm/releaseNonJvm on PangrovaArbitrator
  //   (b) legacy path: non_jvm_allocated_bytes_ atomic counter
  // On free, drain from both paths before forwarding the remainder to
  // Java-side unreserve.
  if (diff < 0) {
    int64_t releaseBytes = -diff;
    int64_t nonJvmDrain = 0;
    // Drain from delegate's non-JVM root pool first (path a).
    // Handles cross-thread frees: allocated via delegate->reserveNonJvm()
    // on a non-JVM thread, freed here on a JVM thread.
    PangrovaArbitrator* delegate =
        non_jvm_root_pool_.load(std::memory_order_acquire);
    if (delegate != nullptr) {
      int64_t delegateDrain = delegate->releaseNonJvm(releaseBytes);
      nonJvmDrain += delegateDrain;
    }
    // CAS loop: drain remaining from legacy non_jvm counter (path b).
    while (nonJvmDrain < releaseBytes) {
      int64_t current = non_jvm_allocated_bytes_.load(std::memory_order_relaxed);
      if (current <= 0) break;
      int64_t drain = std::min(releaseBytes - nonJvmDrain, current);
      if (drain <= 0) break;
      if (non_jvm_allocated_bytes_.compare_exchange_weak(
              current, current - drain, std::memory_order_relaxed)) {
        nonJvmDrain += drain;
        break;
      }
    }
    // Forward remaining release to Java-side unreserve.
    int64_t javaRelease = releaseBytes - nonJvmDrain;
    if (javaRelease > 0) {
      // Task 27.8: RAII guard ensures any pending Java exception (e.g. OOM
      // raised inside unreserve) is cleared + logged before control leaves
      // this scope. Mode::kLog matches the prior behavior — log and
      // continue, since unreserve failures are non-fatal.
      pangrova::jni::JniExceptionScope scope(
          env, "AllocationListener.unreserve",
          pangrova::jni::JniExceptionScope::Mode::kLog);
      env->CallVoidMethod(
          listener_guard_.get(), unreserve_method_, static_cast<jlong>(javaRelease));
      if (scope.hasPending()) {
        PANGROVA_LOG(WARNING) << "event=jni_unreserve_exception"
                  << " release_bytes=" << javaRelease;
      }
    }
    return releaseBytes;
  }

  // Reserve (diff > 0): call Java AllocationListener.reserve(long) -> long.
  // Task 27.8: kSwallow mode — caller (Velox arbitrator) needs a return value,
  // exception is mapped to "denied" by returning 0 below.
  pangrova::jni::JniExceptionScope scope(
      env, "AllocationListener.reserve",
      pangrova::jni::JniExceptionScope::Mode::kSwallow);
  jlong granted = env->CallLongMethod(
      listener_guard_.get(), reserve_method_, static_cast<jlong>(diff));

  if (scope.hasPending()) {
    PANGROVA_LOG(WARNING) << "event=jni_reserve_exception"
              << " requested_bytes=" << diff;
    scope.clear();
    return 0;  // Treat Java exception as denial.
  }

  return static_cast<int64_t>(granted);
}

void PangrovaAllocationListener::setNonJvmRootPoolDelegate(
    PangrovaArbitrator* arbitrator) {
  non_jvm_root_pool_.store(arbitrator, std::memory_order_release);
  PANGROVA_LOG(INFO) << "event=non_jvm_root_pool_delegate_set"
                   << " has_delegate=" << (arbitrator != nullptr);
}

JNIEnv* PangrovaAllocationListener::getEnv() const {
  JNIEnv* env = nullptr;
  jint rc = jvm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);

  if (rc == JNI_OK) {
    return env;
  }

  // Thread is not JVM-attached (e.g. Velox internal I/O / async thread).
  // Return nullptr — caller must handle gracefully without Java callback.
  // We do NOT AttachCurrentThread per the JNI interaction model.
  return nullptr;
}

}  // namespace pangrova::memory
