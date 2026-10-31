// ShuffleEgressExecutor: JNI wrapper implementation for native-to-Java shuffle egress.
//
// Calls Java ShuffleByteEgress.onPartitionCommitted() from the native drain thread.
// Uses DirectByteBuffer for zero-copy byte transfer across JNI boundary.
//
// Pattern follows NativeWriteExecutor: cache method IDs once, hold GlobalRef,
// attach thread on demand, check exceptions after every JNI call.

#include "engine/shuffle/ShuffleEgressExecutor.h"

#include <stdexcept>
#include <folly/ScopeGuard.h>

#include "bridge/JniBridge.h"
#include "bridge/JniExceptionScope.h"
#include "engine/observability/PangrovaLog.h"

namespace pangrova::shuffle {

namespace {

// RAII guard that calls DetachCurrentThread on scope exit when the current
// thread was attached by this guard.
class JniThreadGuard {
 public:
  JniThreadGuard(JavaVM* vm, JNIEnv* env, bool attached)
      : vm_(vm), env_(env), attached_(attached) {}

  ~JniThreadGuard() {
    if (attached_ && vm_) {
      vm_->DetachCurrentThread();
    }
  }

  JNIEnv* env() const { return env_; }
  explicit operator bool() const { return env_ != nullptr; }

  JniThreadGuard(const JniThreadGuard&) = delete;
  JniThreadGuard& operator=(const JniThreadGuard&) = delete;

 private:
  JavaVM* vm_;
  JNIEnv* env_;
  bool attached_;
};

// Acquire a JNIEnv for the current thread, attaching if necessary.
JniThreadGuard acquireEnv(JavaVM* vm) {
  if (!vm) {
    return JniThreadGuard(nullptr, nullptr, false);
  }
  JNIEnv* env = nullptr;
  jint status = vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
  if (status == JNI_OK) {
    return JniThreadGuard(vm, env, false);
  }
  if (status == JNI_EDETACHED) {
    if (vm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) == JNI_OK) {
      return JniThreadGuard(vm, env, true);
    }
  }
  return JniThreadGuard(vm, nullptr, false);
}

}  // namespace

ShuffleEgressExecutor::ShuffleEgressExecutor(JNIEnv* env, jobject egressRef)
    : vm_(nullptr),
      egress_(nullptr),
      onPartitionBatchId_(nullptr),
      onTaskCompletedId_(nullptr),
      onTaskAbortedId_(nullptr) {
  if (!env || !egressRef) {
    throw std::runtime_error(
        "ShuffleEgressExecutor: env and egressRef must not be null");
  }

  if (env->GetJavaVM(&vm_) != JNI_OK || !vm_) {
    throw std::runtime_error("ShuffleEgressExecutor: failed to acquire JavaVM");
  }
  jclass egressClass = env->GetObjectClass(egressRef);
  auto localOwner = folly::makeGuard([&] {
    if (egressClass) env->DeleteLocalRef(egressClass);
  });
  if (!egressClass || env->ExceptionCheck()) {
    throw std::runtime_error(
        "ShuffleEgressExecutor: failed to get class of egress object");
  }
  jobject retained = env->NewGlobalRef(egressRef);
  auto globalOwner = folly::makeGuard([&] {
    if (retained) env->DeleteGlobalRef(retained);
  });
  if (!retained || env->ExceptionCheck()) {
    throw std::runtime_error("ShuffleEgressExecutor: failed to retain egress object");
  }

  onPartitionBatchId_ = env->GetMethodID(
      egressClass, "onPartitionBatch",
      "(Ljava/lang/String;Ljava/lang/String;IJLjava/nio/ByteBuffer;)V");
  if (!onPartitionBatchId_ || env->ExceptionCheck()) {
    throw std::runtime_error("ShuffleEgressExecutor: failed to resolve partition callback");
  }
  onTaskCompletedId_ = env->GetMethodID(
      egressClass, "onTaskCompleted",
      "(Ljava/lang/String;Ljava/lang/String;)V");
  if (!onTaskCompletedId_ || env->ExceptionCheck()) {
    throw std::runtime_error("ShuffleEgressExecutor: failed to resolve completion callback");
  }
  onTaskAbortedId_ = env->GetMethodID(
      egressClass, "onTaskAborted",
      "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
  if (!onTaskAbortedId_ || env->ExceptionCheck()) {
    throw std::runtime_error(
        "ShuffleEgressExecutor: failed to resolve external shuffle callbacks");
  }

  egress_ = retained;
  globalOwner.dismiss();
}

ShuffleEgressExecutor::~ShuffleEgressExecutor() {
  if (egress_ && vm_) {
    auto guard = acquireEnv(vm_);
    JNIEnv* env = guard.env();
    if (env) {
      env->DeleteGlobalRef(egress_);
    }
  }
}

void ShuffleEgressExecutor::onPartitionBatch(
    const std::string& query_id, const std::string& task_id,
    int32_t partition_id, int64_t row_count, const uint8_t* data,
    int64_t size) {
  if (row_count <= 0) {
    throw std::invalid_argument(
        "ShuffleEgressExecutor: row_count must be positive");
  }
  auto guard = acquireEnv(vm_);
  JNIEnv* env = guard.env();
  if (!env) {
    throw std::runtime_error(
        "ShuffleEgressExecutor::onPartitionBatch: no JNIEnv");
  }

  // Create Java String for queryId.
  jstring jQueryId = env->NewStringUTF(query_id.c_str());
  jstring jTaskId = env->NewStringUTF(task_id.c_str());
  if (!jQueryId || !jTaskId) {
    if (jQueryId) env->DeleteLocalRef(jQueryId);
    if (jTaskId) env->DeleteLocalRef(jTaskId);
    throw std::runtime_error(
        "ShuffleEgressExecutor: failed to create task identity strings");
  }

  // Create a DirectByteBuffer wrapping the native memory.
  // The buffer is valid for the duration of this JNI call (the Java side
  // must not retain the ByteBuffer beyond the callback scope).
  jobject jBuffer = env->NewDirectByteBuffer(
      const_cast<uint8_t*>(data), static_cast<jlong>(size));
  if (!jBuffer) {
    env->DeleteLocalRef(jQueryId);
    env->DeleteLocalRef(jTaskId);
    throw std::runtime_error(
        "ShuffleEgressExecutor: failed to create DirectByteBuffer");
  }

  env->CallVoidMethod(
      egress_, onPartitionBatchId_, jQueryId, jTaskId,
      static_cast<jint>(partition_id),
      static_cast<jlong>(row_count),
      jBuffer);

  // Clean up local refs before checking exception.
  env->DeleteLocalRef(jQueryId);
  env->DeleteLocalRef(jTaskId);
  env->DeleteLocalRef(jBuffer);

  checkJniException(env, "onPartitionBatch");
}

void ShuffleEgressExecutor::onTaskCompleted(
    const std::string& query_id, const std::string& task_id) {
  auto guard = acquireEnv(vm_);
  JNIEnv* env = guard.env();
  if (!env) {
    throw std::runtime_error("ShuffleEgressExecutor::onTaskCompleted: no JNIEnv");
  }
  jstring jQueryId = env->NewStringUTF(query_id.c_str());
  jstring jTaskId = env->NewStringUTF(task_id.c_str());
  if (!jQueryId || !jTaskId) {
    if (jQueryId) env->DeleteLocalRef(jQueryId);
    if (jTaskId) env->DeleteLocalRef(jTaskId);
    throw std::runtime_error(
        "ShuffleEgressExecutor: failed to create completion identity strings");
  }
  env->CallVoidMethod(egress_, onTaskCompletedId_, jQueryId, jTaskId);
  env->DeleteLocalRef(jQueryId);
  env->DeleteLocalRef(jTaskId);
  checkJniException(env, "onTaskCompleted");
}

void ShuffleEgressExecutor::onTaskAborted(
    const std::string& query_id, const std::string& task_id,
    const std::string& reason) noexcept {
  try {
    auto guard = acquireEnv(vm_);
    JNIEnv* env = guard.env();
    if (!env) return;
    jstring jQueryId = env->NewStringUTF(query_id.c_str());
    jstring jTaskId = env->NewStringUTF(task_id.c_str());
    jstring jReason = env->NewStringUTF(reason.c_str());
    if (!jQueryId || !jTaskId || !jReason) {
      if (jQueryId) env->DeleteLocalRef(jQueryId);
      if (jTaskId) env->DeleteLocalRef(jTaskId);
      if (jReason) env->DeleteLocalRef(jReason);
      env->ExceptionClear();
      return;
    }
    env->CallVoidMethod(
        egress_, onTaskAbortedId_, jQueryId, jTaskId, jReason);
    env->DeleteLocalRef(jQueryId);
    env->DeleteLocalRef(jTaskId);
    env->DeleteLocalRef(jReason);
    if (env->ExceptionCheck()) env->ExceptionClear();
  } catch (...) {
    // Abort is best-effort while preserving the original Native failure.
  }
}

void ShuffleEgressExecutor::checkJniException(
    JNIEnv* env, const char* context) const {
  if (env->ExceptionCheck()) {
    jthrowable exc = env->ExceptionOccurred();
    env->ExceptionClear();

    std::string msg = "ShuffleEgressExecutor::";
    msg += context;
    msg += ": Java exception";

    if (exc) {
      jclass throwableClass = env->FindClass("java/lang/Throwable");
      if (throwableClass) {
        jmethodID getMsg = env->GetMethodID(
            throwableClass, "getMessage", "()Ljava/lang/String;");
        if (getMsg) {
          auto jmsg = static_cast<jstring>(
              env->CallObjectMethod(exc, getMsg));
          if (jmsg) {
            const char* chars = env->GetStringUTFChars(jmsg, nullptr);
            if (chars) {
              msg += ": ";
              msg += chars;
              env->ReleaseStringUTFChars(jmsg, chars);
            }
            env->DeleteLocalRef(jmsg);
          }
        }
        env->DeleteLocalRef(throwableClass);
      }
      env->DeleteLocalRef(exc);
    }
    throw std::runtime_error(msg);
  }
}

}  // namespace pangrova::shuffle
