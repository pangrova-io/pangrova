#pragma once
#include <jni.h>

#include <string>

#include "jni/bridge/JniReferenceCache.h"
#include "jni/registry/HandleRegistry.h"

namespace pangrova::jni {

// ---------------------------------------------------------------------------
// Global JavaVM* — cached in JNI_OnLoad per pangrova-rules §9.
// ---------------------------------------------------------------------------

// Returns the JavaVM* cached during JNI_OnLoad. Never null after library load.
JavaVM* globalJavaVM();

// Stores the JavaVM* pointer. Called once from JNI_OnLoad.
void cacheJavaVM(JavaVM* vm);

// ---------------------------------------------------------------------------
// Exception helpers
// ---------------------------------------------------------------------------

// Throw a Java exception from C++ side.
// className: fully qualified Java class name with '/' separators.
void throwJavaException(JNIEnv* env, const char* className,
                        const char* message);

// Convenience: throw NativeBridgeException.
// Uses cached class/constructor when available, falls back to FindClass.
void throwNativeBridgeException(JNIEnv* env, const char* message);

// ---------------------------------------------------------------------------
// JNI Local Frame Guard (RAII)
// ---------------------------------------------------------------------------
// Manages PushLocalFrame/PopLocalFrame lifecycle to prevent local reference
// table overflow in JNI methods that create multiple local references.
//
// Usage:
//   JniLocalFrameGuard frame(env, 16);
//   if (!frame.valid()) { throwNativeBridgeException(...); return; }
//   ... create local refs ...
//   return frame.popWithResult(someLocalRef);  // or frame auto-pops nullptr
//
// On destruction (normal or exception), PopLocalFrame is called automatically.
// For methods returning a JNI reference, call popWithResult() to preserve it
// across the frame boundary.
//
// @NotThreadSafe — must be used on the JNI-attached thread that created it.
class JniLocalFrameGuard {
 public:
  // Push a new local reference frame with the given capacity.
  // If PushLocalFrame fails, valid() returns false.
  JniLocalFrameGuard(JNIEnv* env, jint capacity)
      : env_(env), active_(env->PushLocalFrame(capacity) == 0) {}

  ~JniLocalFrameGuard() {
    if (active_) {
      env_->PopLocalFrame(nullptr);
    }
  }

  // Returns true if PushLocalFrame succeeded.
  bool valid() const { return active_; }

  // Pop the frame and preserve a single local reference as the result.
  // The returned reference is valid in the outer frame.
  // After this call, the guard is deactivated (no double-pop).
  jobject popWithResult(jobject result) {
    if (active_) {
      active_ = false;
      return env_->PopLocalFrame(result);
    }
    return nullptr;
  }

  // Non-copyable, non-movable.
  JniLocalFrameGuard(const JniLocalFrameGuard&) = delete;
  JniLocalFrameGuard& operator=(const JniLocalFrameGuard&) = delete;

 private:
  JNIEnv* env_;
  bool active_;
};

}  // namespace pangrova::jni

// JNI function declarations matching NativeBridge.java native methods.
// Package: org.pangrova.worker.jni
extern "C" {

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeInitInternal(
    JNIEnv* env, jclass clazz, jbyteArray configProto, jobject allocationListener);

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeShutdownInternal(
    JNIEnv* env, jclass clazz);

// P1-7: Triggers native pool reclaim (operator MemoryReclaimer spill).
// Called from Java NativeSpillBridge.requestSpill when the Java-side
// arbitrator denies an allocation.
JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeRequestReclaim(
    JNIEnv* env, jclass clazz, jlong targetBytes);

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCreateTaskExecutor0(
    JNIEnv* env, jclass clazz, jbyteArray taskRequestProto);

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCreateTaskExecutorWithTrace0(
    JNIEnv* env, jclass clazz, jbyteArray taskRequestProto, jstring traceId);

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCreateTaskExecutorWithTraceContext0(
    JNIEnv* env, jclass clazz, jbyteArray taskRequestProto, jstring traceId, jstring spanId);

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeStartTask0(
    JNIEnv* env, jclass clazz, jlong executorHandle);

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCancelTask0(
    JNIEnv* env, jclass clazz, jlong executorHandle);

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeReleaseExecutor0(
    JNIEnv* env, jclass clazz, jlong executorHandle);

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetTaskStatus0(
    JNIEnv* env, jclass clazz, jlong executorHandle);

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetTaskStats0(
    JNIEnv* env, jclass clazz, jlong executorHandle);

JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeStartTaskWithTrace0(
    JNIEnv* env, jclass clazz, jlong executorHandle, jstring traceId);

// Unified command channel — all control-plane operations route through here.
JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeCommand0(
    JNIEnv* env, jclass clazz, jlong executorHandle, jbyteArray commandProto);

// Write bridge: serialize Arrow C Data Interface pointers into IPC bytes.
JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeSerializeBatchFromPointers0(
    JNIEnv* env, jclass clazz, jlong arrowArrayPtr, jlong arrowSchemaPtr);

// Write bridge: extract row count from an ArrowArray struct.
JNIEXPORT jint JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetBatchRowCount0(
    JNIEnv* env, jclass clazz, jlong arrowArrayPtr);

// Write bridge: extract column count from an ArrowSchema struct.
JNIEXPORT jint JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetBatchColumnCount0(
    JNIEnv* env, jclass clazz, jlong arrowSchemaPtr);

// Write executor: register Java NativeWriter for C++ write task callbacks.
JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeRegisterWriter0(
    JNIEnv* env, jclass clazz, jobject writer);

// Memory snapshot for heartbeat observability.
JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeGetMemorySnapshot0(
    JNIEnv* env, jclass clazz);

// Shuffle ticket HMAC key injection — sets key(s) for TicketVerifier.
// Called once during WorkerTaskBootstrap startup. Supports dual-key rotation.
JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeSetHmacKey0(
    JNIEnv* env, jclass clazz, jbyteArray currentKey, jbyteArray previousKey);

// Shuffle egress: register Java ShuffleByteEgress for C++ egress callbacks.
// Called once during WorkerTaskBootstrap startup after nativeInit.
// The egress receives committed partition bytes from JVM-resident shuffle providers.
JNIEXPORT void JNICALL
Java_org_pangrova_worker_jni_NativeRuntime_nativeRegisterEgress0(
    JNIEnv* env, jclass clazz, jobject egress);

// ---------------------------------------------------------------------------
// DiskANN JNI entry points (DiskAnnEngine.java)
// ---------------------------------------------------------------------------

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_ai_vector_DiskAnnEngine_nativeBuildIndex(
    JNIEnv* env, jclass clazz,
    jobjectArray vectors, jint dimension, jstring metric,
    jint maxDegree, jint buildListSize, jint pqSubvectors,
    jstring outputDirectory);

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_ai_vector_DiskAnnEngine_nativeSearch(
    JNIEnv* env, jclass clazz,
    jstring indexPath, jfloatArray queryVector, jint topK, jint searchListSize);

// ---------------------------------------------------------------------------
// BM25 Native Scorer JNI entry points (Bm25NativeScorer.java)
// ---------------------------------------------------------------------------

JNIEXPORT jdoubleArray JNICALL
Java_org_pangrova_worker_ai_text_Bm25NativeScorer_nativeScoreBatch(
    JNIEnv* env, jclass clazz,
    jdoubleArray idfs, jintArray termFreqs, jintArray docLengths,
    jint numDocs, jint numTerms, jdouble k1, jdouble b, jdouble avgDocLength);

}  // extern "C"
