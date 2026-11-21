// JniReferenceCache: Caches JNI global references (jclass) and method IDs
// to avoid repeated FindClass/GetMethodID lookups on every JNI call.
//
// Lifecycle: populated in nativeInit, released in nativeShutdown.
// Thread-safe: guarded by internal mutex in JniEntryPoints.cpp.
#pragma once

#include <jni.h>

namespace pangrova::jni {

// Cached JNI references — populated once during nativeInit, reused across
// all subsequent JNI calls. All jclass fields are global refs.
struct JniReferenceCache {
  // org.pangrova.worker.jni.NativeRuntime class.
  jclass native_bridge_class = nullptr;

  // JniCallException class + <init>(String) constructor.
  jclass native_exception_class = nullptr;
  jmethodID exception_constructor = nullptr;

  // org.apache.arrow.c.ArrowArray class + memoryAddress() method.
  jclass arrow_array_class = nullptr;
  jmethodID arrow_array_address = nullptr;

  // org.apache.arrow.c.ArrowSchema class + memoryAddress() method.
  jclass arrow_schema_class = nullptr;
  jmethodID arrow_schema_address = nullptr;
};

// Initialize the global JNI reference cache. Must be called from a thread
// with a valid JNIEnv (e.g. nativeInit). Idempotent — second call is a no-op.
void initJniReferenceCache(JNIEnv* env);

// Release all cached global references. Must be called from nativeShutdown.
void releaseJniReferenceCache(JNIEnv* env);

// Access the global JNI reference cache singleton.
JniReferenceCache& globalJniCache();

}  // namespace pangrova::jni
