// ShuffleEgressExecutor: C++ JNI wrapper for native-to-Java shuffle byte egress.
//
// Wraps JNI calls to Java ShuffleByteEgress.onPartitionCommitted().
// Caches JNI method ID at construction time. Holds a GlobalRef to the
// Java egress instance, released on destruction.
//
// Thread model: native drain thread calls JNI, Java executes on the same thread
// (pull mode per pangrova-rules §9). No async callbacks, no new thread pools.
//
// Error propagation: every JNI call is followed by ExceptionCheck(). If a
// Java exception occurred, it is extracted, cleared, and re-thrown as a
// C++ std::runtime_error so the drain thread can propagate the failure.
//
// Placement: shuffle/ — Native shuffle egress component (ADR-002 D2-egress).
#pragma once

#include <jni.h>

#include <cstdint>
#include <string>

namespace pangrova::shuffle {

// JNI wrapper for Java ShuffleByteEgress. Caches method ID at construction
// and holds a GlobalRef to the Java egress instance.
//
// Lifecycle: one ShuffleEgressExecutor per worker (or per query for isolation).
// The GlobalRef is released in the destructor. Must be destroyed on a
// JNI-attached thread.
class ShuffleEgressExecutor {
 public:
  // Constructs the executor, caching the onPartitionCommitted method ID.
  // Takes a GlobalRef to egressRef (caller may release the local ref).
  // Throws std::runtime_error if method ID lookup fails.
  ShuffleEgressExecutor(JNIEnv* env, jobject egressRef);

  // Releases the GlobalRef to the Java egress instance.
  ~ShuffleEgressExecutor();

  // Non-copyable, non-movable (GlobalRef ownership).
  ShuffleEgressExecutor(const ShuffleEgressExecutor&) = delete;
  ShuffleEgressExecutor& operator=(const ShuffleEgressExecutor&) = delete;
  ShuffleEgressExecutor(ShuffleEgressExecutor&&) = delete;
  ShuffleEgressExecutor& operator=(ShuffleEgressExecutor&&) = delete;

  // Delivers a committed partition segment to the Java egress.
  // Converts the raw byte buffer to a Java DirectByteBuffer for zero-copy.
  // Throws std::runtime_error if the Java side throws.
  void onPartitionCommitted(const std::string& query_id, int32_t stage_id,
                            int32_t partition_id, int32_t map_id,
                            int64_t attempt_id, const uint8_t* data,
                            int64_t size);

 private:
  // Checks for a pending Java exception after a JNI call.
  void checkJniException(JNIEnv* env, const char* context) const;

  JavaVM* vm_;
  jobject egress_;  // GlobalRef to Java ShuffleByteEgress instance

  // Cached method ID for ShuffleByteEgress.onPartitionCommitted(
  //     String queryId, int stageId, int partitionId,
  //     int mapId, long attemptId, ByteBuffer arrowIpc)
  jmethodID onPartitionCommittedId_;
};

}  // namespace pangrova::shuffle
