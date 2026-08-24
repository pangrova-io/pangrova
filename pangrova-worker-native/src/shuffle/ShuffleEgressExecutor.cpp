// ShuffleEgressExecutor: JNI wrapper implementation for native-to-Java shuffle egress.
//
// Calls Java ShuffleByteEgress.onPartitionCommitted() from the native drain thread.
// Uses DirectByteBuffer for zero-copy byte transfer across JNI boundary.
//
// Pattern follows NativeWriteExecutor: cache method IDs once, hold GlobalRef,
// attach thread on demand, check exceptions after every JNI call.

#include "shuffle/ShuffleEgressExecutor.h"

#include <stdexcept>

#include "jni/bridge/JniBridge.h"
#include "jni/bridge/JniExceptionScope.h"
#include "observability/PangrovaLog.h"

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
      onPartitionCommittedId_(nullptr) {
  if (!env || !egressRef) {
    throw std::runtime_error(
        "ShuffleEgressExecutor: env and egressRef must not be null");
  }

  env->GetJavaVM(&vm_);
  egress_ = env->NewGlobalRef(egressRef);

  // Look up the onPartitionCommitted method ID.
  // Signature: (Ljava/lang/String;IIIJLjava/nio/ByteBuffer;)V
  jclass egressClass = env->GetObjectClass(egress_);
  if (!egressClass) {
    env->DeleteGlobalRef(egress_);
    throw std::runtime_error(
        "ShuffleEgressExecutor: failed to get class of egress object");
  }

  onPartitionCommittedId_ = env->GetMethodID(
      egressClass, "onPartitionCommitted",
      "(Ljava/lang/String;IIIJLjava/nio/ByteBuffer;)V");
  if (!onPartitionCommittedId_) {
    env->ExceptionClear();
    env->DeleteGlobalRef(egress_);
    env->DeleteLocalRef(egressClass);
    throw std::runtime_error(
        "ShuffleEgressExecutor: failed to find method "
        "onPartitionCommitted(String,int,int,int,long,ByteBuffer)");
  }

  env->DeleteLocalRef(egressClass);
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

void ShuffleEgressExecutor::onPartitionCommitted(
    const std::string& query_id, int32_t stage_id, int32_t partition_id,
    int32_t map_id, int64_t attempt_id, const uint8_t* data, int64_t size) {
  auto guard = acquireEnv(vm_);
  JNIEnv* env = guard.env();
  if (!env) {
    throw std::runtime_error(
        "ShuffleEgressExecutor::onPartitionCommitted: no JNIEnv");
  }

  // Create Java String for queryId.
  jstring jQueryId = env->NewStringUTF(query_id.c_str());
  if (!jQueryId) {
    throw std::runtime_error(
        "ShuffleEgressExecutor: failed to create queryId string");
  }

  // Create a DirectByteBuffer wrapping the native memory.
  // The buffer is valid for the duration of this JNI call (the Java side
  // must not retain the ByteBuffer beyond the callback scope).
  jobject jBuffer = env->NewDirectByteBuffer(
      const_cast<uint8_t*>(data), static_cast<jlong>(size));
  if (!jBuffer) {
    env->DeleteLocalRef(jQueryId);
    throw std::runtime_error(
        "ShuffleEgressExecutor: failed to create DirectByteBuffer");
  }

  // Call ShuffleByteEgress.onPartitionCommitted(queryId, stageId, partitionId,
  //     mapId, attemptId, arrowIpc)
  env->CallVoidMethod(
      egress_, onPartitionCommittedId_,
      jQueryId,
      static_cast<jint>(stage_id),
      static_cast<jint>(partition_id),
      static_cast<jint>(map_id),
      static_cast<jlong>(attempt_id),
      jBuffer);

  // Clean up local refs before checking exception.
  env->DeleteLocalRef(jQueryId);
  env->DeleteLocalRef(jBuffer);

  checkJniException(env, "onPartitionCommitted");
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
