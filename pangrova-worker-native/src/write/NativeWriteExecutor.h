// NativeWriteExecutor: C++ JNI wrapper for native-to-Java write operations.
//
// Wraps JNI calls to Java NativeWriter:
// beginWrite, appendBatch, commitWrite, abortWrite. Caches JNI method IDs
// at construction time to avoid repeated lookups. Holds a GlobalRef to the
// Java writer instance, released on destruction.
//
// Thread model: native thread calls JNI, Java executes on the same thread
// (pull mode per pangrova-rules §9). No async callbacks, no new thread pools.
//
// Error propagation: every JNI call is followed by ExceptionCheck(). If a
// Java exception occurred, it is extracted, cleared, and re-thrown as a
// C++ std::runtime_error so Velox operator error handling can surface it.
//
// Placement: src/ — top-level write executor component.
#pragma once

#include <jni.h>

#include <cstdint>
#include <string>
#include <vector>

namespace pangrova::write {

// C++ mirror of Java WriteDescriptor — passed to beginWrite().
struct WriteDescriptor {
  std::string catalogName;
  std::string schemaName;
  std::string tableName;
  int32_t writeMode;  // 0=INSERT, 1=OVERWRITE, 2=DELETE
  std::vector<std::pair<std::string, std::string>> columns;  // (name, type)
  std::vector<std::pair<std::string, std::string>> properties;  // (key, value)
};

// C++ mirror of Java WriteCommitResult — returned by commitWrite().
struct WriteCommitResult {
  int64_t rowsWritten;
  int64_t bytesWritten;
  std::vector<std::string> filePathsCreated;
  std::vector<uint8_t> commitMetadata;  // Serialized commit metadata for coordinator-side catalog commit.
};

// JNI wrapper for Java NativeWriter. Caches method IDs at construction
// and holds a GlobalRef to the Java writer instance.
//
// Lifecycle: one NativeWriteExecutor per task. The GlobalRef is released in
// the destructor. Must be destroyed on a JNI-attached thread.
class NativeWriteExecutor {
 public:
  // Constructs the executor, caching all JNI method IDs.
  // Takes a GlobalRef to writerRef (caller may release the local ref).
  // Throws std::runtime_error if any method ID lookup fails.
  NativeWriteExecutor(JNIEnv* env, jobject writerRef);

  // Releases the GlobalRef to the Java writer instance.
  ~NativeWriteExecutor();

  // Non-copyable, non-movable (GlobalRef ownership).
  NativeWriteExecutor(const NativeWriteExecutor&) = delete;
  NativeWriteExecutor& operator=(const NativeWriteExecutor&) = delete;
  NativeWriteExecutor(NativeWriteExecutor&&) = delete;
  NativeWriteExecutor& operator=(NativeWriteExecutor&&) = delete;

  // Opens a write session. Returns an opaque handle for subsequent calls.
  // Throws std::runtime_error if the Java side throws (e.g. table not found).
  int64_t beginWrite(const WriteDescriptor& descriptor);

  // Appends a batch of rows via Arrow C Data Interface pointers.
  // The caller retains ownership of the ArrowArray/ArrowSchema structs.
  // Throws std::runtime_error on Java-side failure.
  // DEPRECATED: Use appendBatchData with pre-serialized Arrow IPC bytes instead.
  void appendBatch(int64_t handle, const void* arrowArray, const void* arrowSchema);

  // Appends a pre-serialized Arrow IPC batch (byte[]) to the write session.
  // Used by drainOutputToWriter to avoid nested JNI callbacks that deadlock
  // on Velox memory pool operations.
  // Throws std::runtime_error on Java-side failure.
  void appendSerializedBatch(int64_t handle, jbyteArray data, int32_t rowCount, int32_t colCount);

  // Appends a pre-serialized Arrow IPC batch from raw memory to the write session.
  // Creates the JNI byte array internally — callers do not need JNIEnv access.
  // This is the primary write path, consistent with the read path.
  // Throws std::runtime_error on failure.
  void appendBatchData(int64_t handle, const uint8_t* data, int64_t size,
                       int32_t rowCount, int32_t colCount);

  // Commits the write session, returning summary statistics.
  // Throws std::runtime_error on Java-side failure.
  WriteCommitResult commitWrite(int64_t handle);

  // Aborts the write session, rolling back partial writes.
  // Does not throw — logs and swallows Java-side abort failures to
  // avoid masking the original exception in catch blocks.
  void abortWrite(int64_t handle) noexcept;

 private:
  // Checks for a pending Java exception after a JNI call. If one exists,
  // extracts the message, clears the exception, and throws std::runtime_error.
  void checkJniException(JNIEnv* env, const char* context) const;

  // Serializes a WriteDescriptor to protobuf bytes for the Java beginWrite().
  static std::vector<uint8_t> serializeDescriptor(const WriteDescriptor& descriptor);

  // Parses a Java WriteCommitResult object into the C++ struct.
  WriteCommitResult parseCommitResult(JNIEnv* env, jobject resultObj) const;

  JavaVM* vm_;
  jobject writer_;  // GlobalRef to Java NativeWriter instance

  // Cached method IDs — looked up once at construction.
  jmethodID beginWriteId_;
  jmethodID appendBatchId_;
  jmethodID appendSerializedBatchId_;
  jmethodID appendDirectBatchId_;  // L4-3A: zero-copy DirectByteBuffer path
  jmethodID commitWriteId_;
  jmethodID abortWriteId_;

  // Cached class/method IDs for WriteDescriptor construction on Java side.
  jclass descriptorClass_;       // GlobalRef
  jmethodID descriptorCtor_;
  jclass columnSchemaClass_;     // GlobalRef
  jmethodID columnSchemaCtor_;
  jclass writeModeClass_;        // GlobalRef
  jmethodID writeModeValues_;

  // Cached field IDs for WriteCommitResult extraction.
  jmethodID commitResultRowsWritten_;
  jmethodID commitResultBytesWritten_;
  jmethodID commitResultFilePaths_;
  jmethodID commitResultCommitMetadata_;

  // Cached JDK collection class/method IDs (per pangrova-rules §9).
  jclass arrayListClass_;        // GlobalRef
  jmethodID arrayListCtor_;
  jmethodID arrayListAdd_;
  jclass hashMapClass_;          // GlobalRef
  jmethodID hashMapCtor_;
  jmethodID hashMapPut_;
  jclass listClass_;             // GlobalRef
  jmethodID listSize_;
  jmethodID listGet_;
  jclass throwableClass_;        // GlobalRef
  jmethodID throwableGetMessage_;
};

}  // namespace pangrova::write
