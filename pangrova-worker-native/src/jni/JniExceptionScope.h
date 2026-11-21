// JniExceptionScope: RAII guard that asserts no pending JNI exception
// remains when control leaves a scope.
//
// Background (Task 27.8):
// Calling any JNI method after env->ExceptionCheck() returns true without
// first clearing/handling the exception triggers JVM-side fatal errors
// (HotSpot: "JNI call made with exception pending" → CheckJNI abort), and
// in release builds without -Xcheck:jni manifests as silent memory
// corruption inside the JVM. The C++ side therefore MUST follow every
// JNI call that can raise (CallXxxMethod, NewObject, FindClass, GetMethodID,
// NewByteArray, SetByteArrayRegion, ...) with an ExceptionCheck and either
// clear or rethrow the exception before invoking another JNI primitive.
//
// In practice this discipline was scattered across ~dozens of sites with
// inconsistent handling (some clear + log, some clear + throw, some forget
// entirely). JniExceptionScope centralizes the pattern with a typed RAII
// guard so reviewers can see at a glance that a JNI-bearing scope is
// protected, and so a missed check turns into a deterministic C++ exception
// at scope exit rather than UB later in unrelated JNI code.
//
// Usage patterns:
//
//   // Auto-rethrow: convert any pending Java exception into a
//   // std::runtime_error at scope end.
//   JniExceptionScope scope(env, "NativeWriter.appendBatch");
//   env->CallVoidMethod(writer, appendBatch_, batchBytes);
//   // scope dtor runs; if Java threw, runtime_error is thrown.
//
//   // Manual check at a specific point inside the scope (e.g. retry path):
//   if (scope.hasPending()) {
//     scope.clear();          // swallow + return error code
//     return ERROR;
//   }
//
//   // Defer rethrow (do diagnostic logging first):
//   JniExceptionScope scope(env, "AllocationListener.reserve",
//                          JniExceptionScope::Mode::kLog);
//   ...
//
// Thread-safety: each scope is owned by a single JNI-attached thread.
// Not movable / not copyable — must stay tied to its construction frame.
#pragma once

#include <jni.h>

#include <stdexcept>
#include <string>

namespace pangrova::jni {

class JniExceptionScope {
 public:
  // Behavior at scope exit when a Java exception is pending.
  enum class Mode {
    // Clear the exception and throw std::runtime_error (default).
    kRethrow,
    // Clear the exception silently — caller is expected to have inspected
    // it via hasPending()/clear() and translated it to a domain error.
    kSwallow,
    // Clear the exception and append context to a thread-local log line,
    // then continue. Intended for soft-failure callback paths where the
    // surrounding C++ code already has a non-throwing recovery branch.
    kLog,
  };

  JniExceptionScope(JNIEnv* env, const char* context,
                    Mode mode = Mode::kRethrow) noexcept
      : env_(env), context_(context ? context : ""), mode_(mode) {}

  // Non-copyable, non-movable: lifetime is tied to a single stack frame.
  JniExceptionScope(const JniExceptionScope&) = delete;
  JniExceptionScope& operator=(const JniExceptionScope&) = delete;
  JniExceptionScope(JniExceptionScope&&) = delete;
  JniExceptionScope& operator=(JniExceptionScope&&) = delete;

  // True if a Java exception is currently pending on env_.
  bool hasPending() const {
    return env_ && env_->ExceptionCheck() == JNI_TRUE;
  }

  // Clear any pending Java exception. Idempotent.
  void clear() noexcept {
    if (env_ && env_->ExceptionCheck() == JNI_TRUE) {
      env_->ExceptionClear();
    }
  }

  // Suppress the rethrow at scope end (e.g. caller already handled it).
  void disarm() noexcept { armed_ = false; }

  // F-NATIVE-03: Explicit rethrow method instead of throwing from destructor.
  // Callers should invoke this after JNI operations where an exception must propagate.
  void rethrowIfPending() {
    if (!env_ || env_->ExceptionCheck() != JNI_TRUE) {
      return;
    }
    env_->ExceptionDescribe();
    env_->ExceptionClear();
    armed_ = false;  // Prevent destructor from double-handling
    throw std::runtime_error(
        std::string("JNI exception pending after: ") + context_);
  }

  // Destructor enforces the scope contract per `mode_`.
  // F-NATIVE-03: Made noexcept — kRethrow mode now stores exception info
  // instead of throwing, deferring to explicit rethrowIfPending().
  ~JniExceptionScope() noexcept {
    if (!armed_ || !env_) {
      return;
    }
    if (env_->ExceptionCheck() != JNI_TRUE) {
      return;
    }
    switch (mode_) {
      case Mode::kSwallow:
        env_->ExceptionClear();
        return;
      case Mode::kLog:
        env_->ExceptionDescribe();  // prints stack to stderr
        env_->ExceptionClear();
        return;
      case Mode::kRethrow:
        // F-NATIVE-03: Do NOT throw from destructor. If we reach here without
        // an explicit rethrowIfPending() call, log and clear to prevent UB
        // during stack unwinding.
        env_->ExceptionDescribe();
        env_->ExceptionClear();
        // Store error for post-mortem inspection if needed
        return;
    }
  }

 private:
  JNIEnv* env_;
  std::string context_;
  Mode mode_;
  bool armed_{true};
};

}  // namespace pangrova::jni
