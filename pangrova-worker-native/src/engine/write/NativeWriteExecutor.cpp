// NativeWriteExecutor: JNI wrapper implementation for native-to-Java write operations.
//
// Each JNI call follows the pattern:
//   1. Get JNIEnv* via cached JavaVM*
//   2. Build Java arguments (convert C++ types to JNI types)
//   3. Call the cached method ID on the writer GlobalRef
//   4. Check ExceptionOccurred() — extract message, clear, throw C++ exception
//   5. Convert Java return value to C++ type
//
// The writer object (Java NativeWriter) is held as a GlobalRef for the
// lifetime of this NativeWriteExecutor instance. All jclass refs used for
// method ID lookups are also held as GlobalRefs and released in the destructor.

#include "engine/write/NativeWriteExecutor.h"

#include <stdexcept>
#include <limits>
#include <utility>

#include "jni/JniEntryPoints.h"
#include "engine/observability/PangrovaLog.h"

namespace pangrova::write {

namespace {

// RAII guard that calls DetachCurrentThread on scope exit when the current
// thread was attached by this guard (per pangrova-rules §9). If the thread
// was already attached to the JVM, the guard is a no-op on destruction.
//
// @NotThreadSafe — must be used on the thread that created it.
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

// Convert a Java jstring to a C++ std::string.
std::string fromJstring(JNIEnv* env, jstring jstr) {
  if (!env || !jstr) {
    return {};
  }
  const char* chars = env->GetStringUTFChars(jstr, nullptr);
  if (!chars) {
    return {};
  }
  std::string result(chars);
  env->ReleaseStringUTFChars(jstr, chars);
  return result;
}

// Convert a C++ std::string to a Java jstring.
jstring toJstring(JNIEnv* env, const std::string& str) {
  return env->NewStringUTF(str.c_str());
}

}  // namespace

NativeWriteExecutor::NativeWriteExecutor(JNIEnv* env, jobject writerRef)
    : vm_(nullptr),
      writer_(nullptr),
      beginWriteId_(nullptr),
      appendDirectBatchId_(nullptr),
      commitWriteId_(nullptr),
      abortWriteId_(nullptr),
      publishCommitMetadataForTaskId_(nullptr),
      descriptorClass_(nullptr),
      descriptorCtor_(nullptr),
      columnSchemaClass_(nullptr),
      columnSchemaCtor_(nullptr),
      writeModeClass_(nullptr),
      dmlOperationFromCode_(nullptr),
      commitResultRowsWritten_(nullptr),
      commitResultBytesWritten_(nullptr),
      commitResultFilePaths_(nullptr),
      commitResultCommitMetadata_(nullptr),
      arrayListClass_(nullptr),
      arrayListCtor_(nullptr),
      arrayListAdd_(nullptr),
      hashMapClass_(nullptr),
      hashMapCtor_(nullptr),
      hashMapPut_(nullptr),
      listClass_(nullptr),
      listSize_(nullptr),
      listGet_(nullptr),
      throwableClass_(nullptr),
      throwableGetMessage_(nullptr) {
  if (!env || !writerRef) {
    throw std::runtime_error(
        "NativeWriteExecutor: env and writerRef must not be null");
  }

  env->GetJavaVM(&vm_);

  // All GlobalRef creation and method ID lookups are wrapped in try-catch.
  // C++ destructors do not run for partially-constructed objects, so we must
  // manually release any GlobalRefs created before the failure point.
  try {
    writer_ = env->NewGlobalRef(writerRef);
    if (!writer_) {
      throw std::runtime_error(
          "NativeWriteExecutor: NewGlobalRef failed for writer");
    }

    // Cache method IDs on the writer class.
    jclass writerClass = env->GetObjectClass(writer_);

    beginWriteId_ = env->GetMethodID(
        writerClass, "beginWrite",
        "(Lorg/pangrova/worker/write/WriteDescriptor;)J");
    if (!beginWriteId_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for beginWrite");
    }

    appendDirectBatchId_ = env->GetMethodID(
        writerClass, "appendDirectBatch", "(JLjava/nio/ByteBuffer;II)V");
    if (!appendDirectBatchId_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for appendDirectBatch");
    }

    commitWriteId_ = env->GetMethodID(
        writerClass, "commitWrite",
        "(J)Lorg/pangrova/worker/write/WriteCommitResult;");
    if (!commitWriteId_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for commitWrite");
    }

    abortWriteId_ = env->GetMethodID(writerClass, "abortWrite", "(J)V");
    if (!abortWriteId_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for abortWrite");
    }

    publishCommitMetadataForTaskId_ = env->GetMethodID(
        writerClass, "publishCommitMetadataForTask", "(Ljava/lang/String;[B)V");
    if (!publishCommitMetadataForTaskId_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for publishCommitMetadataForTask");
    }

    // Cache WriteDescriptor class and constructor.
    jclass descLocal = env->FindClass(
        "org/pangrova/worker/write/WriteDescriptor");
    if (!descLocal) {
      throw std::runtime_error(
          "NativeWriteExecutor: FindClass failed for WriteDescriptor");
    }
    descriptorClass_ = static_cast<jclass>(env->NewGlobalRef(descLocal));
    env->DeleteLocalRef(descLocal);

    descriptorCtor_ = env->GetMethodID(
        descriptorClass_, "<init>",
        "(Lorg/pangrova/worker/enums/DmlOperation;"
        "Ljava/util/List;Ljava/util/Map;)V");
    if (!descriptorCtor_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for WriteDescriptor ctor");
    }

    // Cache ColumnSchema class and constructor (inner record of WriteDescriptor).
    jclass colLocal = env->FindClass(
        "org/pangrova/worker/write/WriteDescriptor$ColumnSchema");
    if (!colLocal) {
      throw std::runtime_error(
          "NativeWriteExecutor: FindClass failed for WriteDescriptor$ColumnSchema");
    }
    columnSchemaClass_ = static_cast<jclass>(env->NewGlobalRef(colLocal));
    env->DeleteLocalRef(colLocal);

    columnSchemaCtor_ = env->GetMethodID(
        columnSchemaClass_, "<init>",
        "(Ljava/lang/String;Ljava/lang/String;)V");
    if (!columnSchemaCtor_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for ColumnSchema ctor");
    }

    // Resolve the versioned native-write operation codes independently of Java enum order.
    jclass modeLocal = env->FindClass(
        "org/pangrova/worker/enums/DmlOperation");
    if (!modeLocal) {
      throw std::runtime_error(
          "NativeWriteExecutor: FindClass failed for DmlOperation");
    }
    writeModeClass_ = static_cast<jclass>(env->NewGlobalRef(modeLocal));
    env->DeleteLocalRef(modeLocal);

    dmlOperationFromCode_ = env->GetStaticMethodID(
        writeModeClass_, "fromNativeCode",
        "(I)Lorg/pangrova/worker/enums/DmlOperation;");
    if (!dmlOperationFromCode_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetStaticMethodID failed for DmlOperation.fromNativeCode(int)");
    }

    // Cache WriteCommitResult accessor method IDs.
    jclass resultLocal = env->FindClass(
        "org/pangrova/worker/write/WriteCommitResult");
    if (!resultLocal) {
      throw std::runtime_error(
          "NativeWriteExecutor: FindClass failed for WriteCommitResult");
    }

    commitResultRowsWritten_ = env->GetMethodID(
        resultLocal, "rowsWritten", "()J");
    commitResultBytesWritten_ = env->GetMethodID(
        resultLocal, "bytesWritten", "()J");
    commitResultFilePaths_ = env->GetMethodID(
        resultLocal, "filePathsCreated", "()Ljava/util/List;");
    commitResultCommitMetadata_ = env->GetMethodID(
        resultLocal, "commitMetadata", "()[B");
    env->DeleteLocalRef(resultLocal);

    if (!commitResultRowsWritten_ || !commitResultBytesWritten_ ||
        !commitResultFilePaths_ || !commitResultCommitMetadata_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for WriteCommitResult accessors");
    }

    env->DeleteLocalRef(writerClass);

    // Cache JDK collection classes and method IDs (per pangrova-rules §9).
    jclass alLocal = env->FindClass("java/util/ArrayList");
    if (!alLocal) {
      throw std::runtime_error(
          "NativeWriteExecutor: FindClass failed for java/util/ArrayList");
    }
    arrayListClass_ = static_cast<jclass>(env->NewGlobalRef(alLocal));
    env->DeleteLocalRef(alLocal);
    arrayListCtor_ = env->GetMethodID(arrayListClass_, "<init>", "(I)V");
    arrayListAdd_ = env->GetMethodID(
        arrayListClass_, "add", "(Ljava/lang/Object;)Z");
    if (!arrayListCtor_ || !arrayListAdd_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for ArrayList");
    }

    jclass hmLocal = env->FindClass("java/util/HashMap");
    if (!hmLocal) {
      throw std::runtime_error(
          "NativeWriteExecutor: FindClass failed for java/util/HashMap");
    }
    hashMapClass_ = static_cast<jclass>(env->NewGlobalRef(hmLocal));
    env->DeleteLocalRef(hmLocal);
    hashMapCtor_ = env->GetMethodID(hashMapClass_, "<init>", "(I)V");
    hashMapPut_ = env->GetMethodID(
        hashMapClass_, "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    if (!hashMapCtor_ || !hashMapPut_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for HashMap");
    }

    jclass listLocal = env->FindClass("java/util/List");
    if (!listLocal) {
      throw std::runtime_error(
          "NativeWriteExecutor: FindClass failed for java/util/List");
    }
    listClass_ = static_cast<jclass>(env->NewGlobalRef(listLocal));
    env->DeleteLocalRef(listLocal);
    listSize_ = env->GetMethodID(listClass_, "size", "()I");
    listGet_ = env->GetMethodID(
        listClass_, "get", "(I)Ljava/lang/Object;");
    if (!listSize_ || !listGet_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for List");
    }

    jclass thLocal = env->FindClass("java/lang/Throwable");
    if (!thLocal) {
      throw std::runtime_error(
          "NativeWriteExecutor: FindClass failed for java/lang/Throwable");
    }
    throwableClass_ = static_cast<jclass>(env->NewGlobalRef(thLocal));
    env->DeleteLocalRef(thLocal);
    throwableGetMessage_ = env->GetMethodID(
        throwableClass_, "getMessage", "()Ljava/lang/String;");
    if (!throwableGetMessage_) {
      throw std::runtime_error(
          "NativeWriteExecutor: GetMethodID failed for Throwable.getMessage");
    }
  } catch (...) {
    // Clean up any GlobalRefs that were successfully created before failure.
    if (writer_) { env->DeleteGlobalRef(writer_); writer_ = nullptr; }
    if (descriptorClass_) { env->DeleteGlobalRef(descriptorClass_); descriptorClass_ = nullptr; }
    if (columnSchemaClass_) { env->DeleteGlobalRef(columnSchemaClass_); columnSchemaClass_ = nullptr; }
    if (writeModeClass_) { env->DeleteGlobalRef(writeModeClass_); writeModeClass_ = nullptr; }
    if (arrayListClass_) { env->DeleteGlobalRef(arrayListClass_); arrayListClass_ = nullptr; }
    if (hashMapClass_) { env->DeleteGlobalRef(hashMapClass_); hashMapClass_ = nullptr; }
    if (listClass_) { env->DeleteGlobalRef(listClass_); listClass_ = nullptr; }
    if (throwableClass_) { env->DeleteGlobalRef(throwableClass_); throwableClass_ = nullptr; }
    throw;
  }
}

NativeWriteExecutor::~NativeWriteExecutor() {
  auto guard = acquireEnv(vm_);
  JNIEnv* env = guard.env();
  if (!env) {
    // Cannot obtain JNIEnv — JVM may be shutting down or thread cannot
    // attach. GlobalRefs will leak, but this is preferable to crashing.
    PANGROVA_LOG_WARN << "event=write_executor_destructor_no_env"
                    << " leaked_global_refs=8";
    return;
  }
  if (writer_) {
    env->DeleteGlobalRef(writer_);
  }
  if (descriptorClass_) {
    env->DeleteGlobalRef(descriptorClass_);
  }
  if (columnSchemaClass_) {
    env->DeleteGlobalRef(columnSchemaClass_);
  }
  if (writeModeClass_) {
    env->DeleteGlobalRef(writeModeClass_);
  }
  if (arrayListClass_) {
    env->DeleteGlobalRef(arrayListClass_);
  }
  if (hashMapClass_) {
    env->DeleteGlobalRef(hashMapClass_);
  }
  if (listClass_) {
    env->DeleteGlobalRef(listClass_);
  }
  if (throwableClass_) {
    env->DeleteGlobalRef(throwableClass_);
  }
}

void NativeWriteExecutor::checkJniException(JNIEnv* env, const char* context) const {
  if (!env->ExceptionCheck()) {
    return;
  }
  jthrowable exc = env->ExceptionOccurred();
  env->ExceptionClear();

  std::string message = "JNI exception in ";
  message += context;

  if (exc) {
    if (throwableGetMessage_) {
      auto jmsg = static_cast<jstring>(
          env->CallObjectMethod(exc, throwableGetMessage_));
      if (jmsg && !env->ExceptionCheck()) {
        message += ": " + fromJstring(env, jmsg);
        env->DeleteLocalRef(jmsg);
      } else {
        env->ExceptionClear();
      }
    }
    env->DeleteLocalRef(exc);
  }

  throw std::runtime_error(message);
}

int64_t NativeWriteExecutor::beginWrite(const WriteDescriptor& descriptor) {
  auto guard = acquireEnv(vm_);
  JNIEnv* env = guard.env();
  if (!env) {
    throw std::runtime_error("NativeWriteExecutor::beginWrite: no JNIEnv");
  }

  // Local frame guard: beginWrite creates many local refs (jstrings,
  // ArrayList, HashMap, WriteDescriptor, ColumnSchema, DmlOperation).
  // Capacity 32 covers worst case for large column/property lists.
  pangrova::jni::JniLocalFrameGuard frame(env, 32);
  if (!frame.valid()) {
    throw std::runtime_error(
        "NativeWriteExecutor::beginWrite: PushLocalFrame failed");
  }

  jobject writeMode = env->CallStaticObjectMethod(
      writeModeClass_, dmlOperationFromCode_, static_cast<jint>(descriptor.writeMode));
  checkJniException(env, "DmlOperation.fromNativeCode(int)");

  // Build Java List<ColumnSchema>.
  jobject columnList = env->NewObject(
      arrayListClass_, arrayListCtor_,
      static_cast<jint>(descriptor.columns.size()));
  for (const auto& [name, type] : descriptor.columns) {
    jstring jn = toJstring(env, name);
    jstring jt = toJstring(env, type);
    jobject col = env->NewObject(columnSchemaClass_, columnSchemaCtor_, jn, jt);
    checkJniException(env, "ColumnSchema ctor");
    env->CallBooleanMethod(columnList, arrayListAdd_, col);
    checkJniException(env, "ArrayList.add");
    env->DeleteLocalRef(col);
    env->DeleteLocalRef(jn);
    env->DeleteLocalRef(jt);
  }

  // Build Java Map<String, String>.
  jobject propsMap = env->NewObject(
      hashMapClass_, hashMapCtor_,
      static_cast<jint>(descriptor.properties.size()));
  for (const auto& [key, value] : descriptor.properties) {
    jstring jk = toJstring(env, key);
    jstring jv = toJstring(env, value);
    env->CallObjectMethod(propsMap, hashMapPut_, jk, jv);
    checkJniException(env, "HashMap.put");
    env->DeleteLocalRef(jk);
    env->DeleteLocalRef(jv);
  }

  // Construct Java WriteDescriptor record.
  jobject jDesc = env->NewObject(
      descriptorClass_, descriptorCtor_,
      writeMode, columnList, propsMap);
  checkJniException(env, "WriteDescriptor ctor");

  jlong handle = env->CallLongMethod(writer_, beginWriteId_, jDesc);
  checkJniException(env, "beginWrite");

  // JniLocalFrameGuard auto-releases all local refs on scope exit.
  return static_cast<int64_t>(handle);
}

void NativeWriteExecutor::appendBatchData(
    int64_t handle, const uint8_t* data, int64_t size,
    int32_t rowCount, int32_t colCount) {
  auto guard = acquireEnv(vm_);
  JNIEnv* env = guard.env();
  if (!env) {
    throw std::runtime_error(
        "NativeWriteExecutor::appendBatchData: no JNIEnv");
  }

  if (data == nullptr || size <= 0 ||
      size > std::numeric_limits<int32_t>::max() ||
      rowCount < 0 || colCount <= 0) {
    throw std::invalid_argument(
        "NativeWriteExecutor::appendBatchData: invalid batch bounds");
  }
  jobject directBuffer = env->NewDirectByteBuffer(
      const_cast<uint8_t*>(data), static_cast<jlong>(size));
  checkJniException(env, "NewDirectByteBuffer");
  if (!directBuffer) {
    throw std::runtime_error(
        "NativeWriteExecutor::appendBatchData: direct buffer allocation failed");
  }
  env->CallVoidMethod(
      writer_, appendDirectBatchId_,
      static_cast<jlong>(handle), directBuffer,
      static_cast<jint>(rowCount), static_cast<jint>(colCount));
  env->DeleteLocalRef(directBuffer);
  checkJniException(env, "appendDirectBatch");
}

WriteCommitResult NativeWriteExecutor::commitWrite(int64_t handle) {
  auto guard = acquireEnv(vm_);
  JNIEnv* env = guard.env();
  if (!env) {
    throw std::runtime_error("NativeWriteExecutor::commitWrite: no JNIEnv");
  }

  jobject resultObj = env->CallObjectMethod(
      writer_, commitWriteId_, static_cast<jlong>(handle));
  checkJniException(env, "commitWrite");

  WriteCommitResult result = parseCommitResult(env, resultObj);
  env->DeleteLocalRef(resultObj);
  return result;
}

void NativeWriteExecutor::publishCommitMetadataForTask(
    const std::string& taskId,
    const std::vector<uint8_t>& metadata) {
  if (taskId.empty() || metadata.empty() ||
      metadata.size() > static_cast<size_t>(std::numeric_limits<jsize>::max())) {
    throw std::invalid_argument(
        "NativeWriteExecutor::publishCommitMetadataForTask: invalid task metadata");
  }
  auto guard = acquireEnv(vm_);
  JNIEnv* env = guard.env();
  if (!env) {
    throw std::runtime_error(
        "NativeWriteExecutor::publishCommitMetadataForTask: no JNIEnv");
  }
  jstring task = toJstring(env, taskId);
  checkJniException(env, "publishCommitMetadataForTask.NewStringUTF");
  if (!task) {
    throw std::runtime_error(
        "NativeWriteExecutor::publishCommitMetadataForTask: task string allocation failed");
  }
  jbyteArray bytes = env->NewByteArray(static_cast<jsize>(metadata.size()));
  if (!bytes) {
    env->DeleteLocalRef(task);
    checkJniException(env, "publishCommitMetadataForTask.NewByteArray");
    throw std::runtime_error(
        "NativeWriteExecutor::publishCommitMetadataForTask: byte array allocation failed");
  }
  env->SetByteArrayRegion(
      bytes, 0, static_cast<jsize>(metadata.size()),
      reinterpret_cast<const jbyte*>(metadata.data()));
  checkJniException(env, "publishCommitMetadataForTask.SetByteArrayRegion");
  env->CallVoidMethod(writer_, publishCommitMetadataForTaskId_, task, bytes);
  env->DeleteLocalRef(bytes);
  env->DeleteLocalRef(task);
  checkJniException(env, "publishCommitMetadataForTask");
}

void NativeWriteExecutor::abortWrite(int64_t handle) {
  auto guard = acquireEnv(vm_);
  JNIEnv* env = guard.env();
  if (!env) {
    throw std::runtime_error("NativeWriteExecutor::abortWrite: no attached JNIEnv");
  }
  env->CallVoidMethod(writer_, abortWriteId_, static_cast<jlong>(handle));
  checkJniException(env, "NativeWriter.abortWrite");
}

WriteCommitResult NativeWriteExecutor::parseCommitResult(
    JNIEnv* env, jobject resultObj) const {
  if (!resultObj) {
    throw std::runtime_error("NativeWriteExecutor: commitWrite returned null");
  }

  WriteCommitResult result;
  result.rowsWritten = env->CallLongMethod(
      resultObj, commitResultRowsWritten_);
  result.bytesWritten = env->CallLongMethod(
      resultObj, commitResultBytesWritten_);

  auto fileList = env->CallObjectMethod(resultObj, commitResultFilePaths_);
  if (fileList) {
    jint size = env->CallIntMethod(fileList, listSize_);

    result.filePathsCreated.reserve(static_cast<size_t>(size));
    for (jint i = 0; i < size; ++i) {
      auto jpath = static_cast<jstring>(
          env->CallObjectMethod(fileList, listGet_, i));
      result.filePathsCreated.push_back(fromJstring(env, jpath));
      env->DeleteLocalRef(jpath);
    }
    env->DeleteLocalRef(fileList);
  }

  // Extract commitMetadata (byte[]) for coordinator-side catalog commit.
  auto metadataArray = static_cast<jbyteArray>(
      env->CallObjectMethod(resultObj, commitResultCommitMetadata_));
  if (metadataArray) {
    jsize len = env->GetArrayLength(metadataArray);
    if (len > 0) {
      result.commitMetadata.resize(static_cast<size_t>(len));
      env->GetByteArrayRegion(
          metadataArray, 0, len,
          reinterpret_cast<jbyte*>(result.commitMetadata.data()));
    }
    env->DeleteLocalRef(metadataArray);
  }

  return result;
}

}  // namespace pangrova::write
