// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "udf/JvmScalarFunction.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "jni/JniEntryPoints.h"
#include "command/resource/HandleRegistry.h"
#include "pangrova/protocol/v2/common.pb.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "StructuralContractIdentities.h"

namespace pangrova::udf {
namespace {

using namespace facebook::velox;

constexpr size_t kFileFieldCount = 5;
constexpr size_t kGovernedHiddenArguments = pangrova::capability::generated::AI_MODEL.size();
constexpr size_t governedOffset(std::string_view name) {
  return pangrova::capability::generated::governedOrdinal(pangrova::capability::generated::AI_MODEL, name);
}
constexpr size_t kVectorDimensionOffset = governedOffset("output_vector_dimension");

std::string upper(std::string value) {
  std::transform(
      value.begin(), value.end(), value.begin(),
      [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return value;
}

TypePtr fileType() {
  return ROW(
      {"uri", "mime_type", "size", "checksum", "created_at"},
      {VARCHAR(), VARCHAR(), BIGINT(), VARCHAR(), TIMESTAMP()});
}

TypePtr parseType(const std::string& declaredType) {
  const auto type = upper(declaredType);
  if (type == "BOOLEAN") return BOOLEAN();
  if (type == "TINYINT") return TINYINT();
  if (type == "SMALLINT") return SMALLINT();
  if (type == "INTEGER" || type == "INT") return INTEGER();
  if (type == "BIGINT") return BIGINT();
  if (type == "REAL" || type == "FLOAT") return REAL();
  if (type == "DOUBLE") return DOUBLE();
  if (type == "VARCHAR" || type == "STRING") return VARCHAR();
  if (type == "VARBINARY" || type == "BINARY") return VARBINARY();
  if (type == "VECTOR") return ARRAY(REAL());
  if (type == "ROW" || type == "FILE") return fileType();
  throw std::invalid_argument(
      "Unsupported JVM scalar function type: " + declaredType);
}

class ScopedJniEnv {
 public:
  ScopedJniEnv() : vm_(pangrova::jni::globalJavaVM()) {
    if (!vm_) {
      throw std::runtime_error("JVM scalar function has no JavaVM");
    }
    jint status = vm_->GetEnv(
        reinterpret_cast<void**>(&env_), JNI_VERSION_1_8);
    if (status == JNI_EDETACHED) {
      if (vm_->AttachCurrentThread(
              reinterpret_cast<void**>(&env_), nullptr) != JNI_OK) {
        throw std::runtime_error(
            "JVM scalar function failed to attach execution thread");
      }
      attached_ = true;
    } else if (status != JNI_OK) {
      throw std::runtime_error("JVM scalar function failed to acquire JNIEnv");
    }
  }

  ~ScopedJniEnv() {
    if (attached_) {
      vm_->DetachCurrentThread();
    }
  }

  JNIEnv* get() const { return env_; }

  ScopedJniEnv(const ScopedJniEnv&) = delete;
  ScopedJniEnv& operator=(const ScopedJniEnv&) = delete;

 private:
  JavaVM* vm_ = nullptr;
  JNIEnv* env_ = nullptr;
  bool attached_ = false;
};

struct CallbackSnapshot {
  jobject callback = nullptr;
  jmethodID evaluateBatch = nullptr;
  jmethodID describeAiFailure = nullptr;
  jclass objectClass = nullptr;
  jclass objectArrayClass = nullptr;
  jclass stringClass = nullptr;
  jclass booleanClass = nullptr;
  jclass integerClass = nullptr;
  jclass longClass = nullptr;
  jclass floatClass = nullptr;
  jclass doubleClass = nullptr;
  jmethodID stringConstructor = nullptr;
  jmethodID stringGetBytes = nullptr;
  jmethodID booleanValueOf = nullptr;
  jmethodID integerValueOf = nullptr;
  jmethodID longValueOf = nullptr;
  jmethodID floatValueOf = nullptr;
  jmethodID doubleValueOf = nullptr;
  jmethodID booleanValue = nullptr;
  jmethodID integerValue = nullptr;
  jmethodID longValue = nullptr;
  jmethodID floatValue = nullptr;
  jmethodID doubleValue = nullptr;
};

}  // namespace

class JvmFunctionCallbacks {
 public:
  ~JvmFunctionCallbacks() {
    // Runtime shutdown drains execution before the JVM is unloaded. Factories
    // and in-flight batches may retain this generation beyond active publication.
    if (pangrova::jni::globalJavaVM()) {
      try {
        ScopedJniEnv scopedEnv;
        clearLocked(scopedEnv.get());
      } catch (...) {
        // Destruction cannot throw. If the VM can no longer attach, the VM owns
        // its remaining reference reclamation; never invoke a dangling JNIEnv.
      }
    }
  }

  void install(JNIEnv* env, jobject callback) {
    if (!callback) {
      throw std::invalid_argument("JVM function callback must not be null");
    }
    try {
      callback_ = env->NewGlobalRef(callback);
      if (!callback_) {
        throw std::runtime_error("Failed to retain JVM function callback");
      }
      callbackClass_ = globalClass(env, "org/pangrova/worker/function/NativeFunctionCallbacks");
      objectClass_ = globalClass(env, "java/lang/Object");
      objectArrayClass_ = globalClass(env, "[Ljava/lang/Object;");
      stringClass_ = globalClass(env, "java/lang/String");
      booleanClass_ = globalClass(env, "java/lang/Boolean");
      integerClass_ = globalClass(env, "java/lang/Integer");
      longClass_ = globalClass(env, "java/lang/Long");
      floatClass_ = globalClass(env, "java/lang/Float");
      doubleClass_ = globalClass(env, "java/lang/Double");
      evaluateBatch_ = requireMethod(
          env, callbackClass_, "evaluateBatch",
          "(Ljava/lang/String;[[Ljava/lang/Object;[B)[Ljava/lang/Object;");
      describeAiFailure_ = requireMethod(
          env, callbackClass_, "describeAiFailure", "(Ljava/lang/Throwable;)[B");
      stringConstructor_ = requireMethod(
          env, stringClass_, "<init>", "([BLjava/lang/String;)V");
      stringGetBytes_ = requireMethod(
          env, stringClass_, "getBytes", "(Ljava/lang/String;)[B");
      booleanValueOf_ = requireStaticMethod(
          env, booleanClass_, "valueOf", "(Z)Ljava/lang/Boolean;");
      integerValueOf_ = requireStaticMethod(
          env, integerClass_, "valueOf", "(I)Ljava/lang/Integer;");
      longValueOf_ = requireStaticMethod(
          env, longClass_, "valueOf", "(J)Ljava/lang/Long;");
      floatValueOf_ = requireStaticMethod(
          env, floatClass_, "valueOf", "(F)Ljava/lang/Float;");
      doubleValueOf_ = requireStaticMethod(
          env, doubleClass_, "valueOf", "(D)Ljava/lang/Double;");
      booleanValue_ = requireMethod(env, booleanClass_, "booleanValue", "()Z");
      integerValue_ = requireMethod(env, integerClass_, "intValue", "()I");
      longValue_ = requireMethod(env, longClass_, "longValue", "()J");
      floatValue_ = requireMethod(env, floatClass_, "floatValue", "()F");
      doubleValue_ = requireMethod(env, doubleClass_, "doubleValue", "()D");
    } catch (...) {
      clearLocked(env);
      throw;
    }
  }

  CallbackSnapshot snapshot(JNIEnv* env) const {
    if (!callback_ || !evaluateBatch_ || !describeAiFailure_) {
      throw std::runtime_error("JVM function callback is not installed");
    }
    CallbackSnapshot result;
    result.callback = env->NewLocalRef(callback_);
    if (!result.callback) {
      throw std::runtime_error("Failed to retain JVM function callback");
    }
    result.evaluateBatch = evaluateBatch_;
    result.describeAiFailure = describeAiFailure_;
    result.objectClass = objectClass_;
    result.objectArrayClass = objectArrayClass_;
    result.stringClass = stringClass_;
    result.booleanClass = booleanClass_;
    result.integerClass = integerClass_;
    result.longClass = longClass_;
    result.floatClass = floatClass_;
    result.doubleClass = doubleClass_;
    result.stringConstructor = stringConstructor_;
    result.stringGetBytes = stringGetBytes_;
    result.booleanValueOf = booleanValueOf_;
    result.integerValueOf = integerValueOf_;
    result.longValueOf = longValueOf_;
    result.floatValueOf = floatValueOf_;
    result.doubleValueOf = doubleValueOf_;
    result.booleanValue = booleanValue_;
    result.integerValue = integerValue_;
    result.longValue = longValue_;
    result.floatValue = floatValue_;
    result.doubleValue = doubleValue_;
    return result;
  }

 private:
  static jclass globalClass(JNIEnv* env, const char* name) {
    jclass local = env->FindClass(name);
    if (!local) {
      throw std::runtime_error(std::string("JNI class not found: ") + name);
    }
    auto global = static_cast<jclass>(env->NewGlobalRef(local));
    env->DeleteLocalRef(local);
    if (!global) {
      throw std::runtime_error(
          std::string("JNI global class allocation failed: ") + name);
    }
    return global;
  }

  static jmethodID requireMethod(
      JNIEnv* env, jclass type, const char* name, const char* signature) {
    jmethodID method = env->GetMethodID(type, name, signature);
    if (!method) {
      throw std::runtime_error(std::string("JNI method not found: ") + name);
    }
    return method;
  }

  static jmethodID requireStaticMethod(
      JNIEnv* env, jclass type, const char* name, const char* signature) {
    jmethodID method = env->GetStaticMethodID(type, name, signature);
    if (!method) {
      throw std::runtime_error(
          std::string("JNI static method not found: ") + name);
    }
    return method;
  }

  static void deleteRef(JNIEnv* env, jobject& ref) {
    if (ref) {
      env->DeleteGlobalRef(ref);
      ref = nullptr;
    }
  }

  static void deleteClass(JNIEnv* env, jclass& ref) {
    if (ref) {
      env->DeleteGlobalRef(ref);
      ref = nullptr;
    }
  }

  void clearLocked(JNIEnv* env) {
    deleteRef(env, callback_);
    deleteClass(env, callbackClass_);
    deleteClass(env, objectClass_);
    deleteClass(env, objectArrayClass_);
    deleteClass(env, stringClass_);
    deleteClass(env, booleanClass_);
    deleteClass(env, integerClass_);
    deleteClass(env, longClass_);
    deleteClass(env, floatClass_);
    deleteClass(env, doubleClass_);
    evaluateBatch_ = nullptr;
    describeAiFailure_ = nullptr;
    stringConstructor_ = nullptr;
    stringGetBytes_ = nullptr;
    booleanValueOf_ = nullptr;
    integerValueOf_ = nullptr;
    longValueOf_ = nullptr;
    floatValueOf_ = nullptr;
    doubleValueOf_ = nullptr;
    booleanValue_ = nullptr;
    integerValue_ = nullptr;
    longValue_ = nullptr;
    floatValue_ = nullptr;
    doubleValue_ = nullptr;
  }

  jobject callback_ = nullptr;
  jclass callbackClass_ = nullptr;
  jclass objectClass_ = nullptr;
  jclass objectArrayClass_ = nullptr;
  jclass stringClass_ = nullptr;
  jclass booleanClass_ = nullptr;
  jclass integerClass_ = nullptr;
  jclass longClass_ = nullptr;
  jclass floatClass_ = nullptr;
  jclass doubleClass_ = nullptr;
  jmethodID evaluateBatch_ = nullptr;
  jmethodID describeAiFailure_ = nullptr;
  jmethodID stringConstructor_ = nullptr;
  jmethodID stringGetBytes_ = nullptr;
  jmethodID booleanValueOf_ = nullptr;
  jmethodID integerValueOf_ = nullptr;
  jmethodID longValueOf_ = nullptr;
  jmethodID floatValueOf_ = nullptr;
  jmethodID doubleValueOf_ = nullptr;
  jmethodID booleanValue_ = nullptr;
  jmethodID integerValue_ = nullptr;
  jmethodID longValue_ = nullptr;
  jmethodID floatValue_ = nullptr;
  jmethodID doubleValue_ = nullptr;
};

namespace {

std::mutex callbackPublicationMutex;
JvmFunctionCallbackOwner activeCallbacks;

std::string consumeJavaException(JNIEnv* env) {
  jthrowable failure = env->ExceptionOccurred();
  env->ExceptionClear();
  if (!failure) return "unknown Java exception";
  jclass failureClass = env->GetObjectClass(failure);
  jmethodID toString = env->GetMethodID(
      failureClass, "toString", "()Ljava/lang/String;");
  auto message = static_cast<jstring>(env->CallObjectMethod(failure, toString));
  if (env->ExceptionCheck() || !message) {
    env->ExceptionClear();
    return "Java function callback failed";
  }
  const char* chars = env->GetStringUTFChars(message, nullptr);
  std::string result = chars ? chars : "Java function callback failed";
  if (chars) env->ReleaseStringUTFChars(message, chars);
  return result;
}

jstring newUtf8String(
    JNIEnv* env,
    const CallbackSnapshot& callback,
    const char* data,
    size_t size) {
  auto bytes = env->NewByteArray(static_cast<jsize>(size));
  if (!bytes) throw std::runtime_error("Failed to allocate Java UTF-8 bytes");
  if (size > 0) {
    env->SetByteArrayRegion(
        bytes, 0, static_cast<jsize>(size),
        reinterpret_cast<const jbyte*>(data));
  }
  jstring charset = env->NewStringUTF("UTF-8");
  auto result = static_cast<jstring>(env->NewObject(
      callback.stringClass, callback.stringConstructor, bytes, charset));
  env->DeleteLocalRef(bytes);
  env->DeleteLocalRef(charset);
  if (env->ExceptionCheck()) {
    throw std::runtime_error(consumeJavaException(env));
  }
  return result;
}

jobject scalarToJava(
    JNIEnv* env,
    const CallbackSnapshot& callback,
    const DecodedVector& decoded,
    vector_size_t row,
    TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
      return env->CallStaticObjectMethod(
          callback.booleanClass, callback.booleanValueOf,
          static_cast<jboolean>(decoded.valueAt<bool>(row)));
    case TypeKind::TINYINT:
      return env->CallStaticObjectMethod(
          callback.integerClass, callback.integerValueOf,
          static_cast<jint>(decoded.valueAt<int8_t>(row)));
    case TypeKind::SMALLINT:
      return env->CallStaticObjectMethod(
          callback.integerClass, callback.integerValueOf,
          static_cast<jint>(decoded.valueAt<int16_t>(row)));
    case TypeKind::INTEGER:
      return env->CallStaticObjectMethod(
          callback.integerClass, callback.integerValueOf,
          static_cast<jint>(decoded.valueAt<int32_t>(row)));
    case TypeKind::BIGINT:
      return env->CallStaticObjectMethod(
          callback.longClass, callback.longValueOf,
          static_cast<jlong>(decoded.valueAt<int64_t>(row)));
    case TypeKind::TIMESTAMP:
      return env->CallStaticObjectMethod(
          callback.longClass, callback.longValueOf,
          static_cast<jlong>(decoded.valueAt<Timestamp>(row).toMicros()));
    case TypeKind::REAL:
      return env->CallStaticObjectMethod(
          callback.floatClass, callback.floatValueOf,
          static_cast<jfloat>(decoded.valueAt<float>(row)));
    case TypeKind::DOUBLE:
      return env->CallStaticObjectMethod(
          callback.doubleClass, callback.doubleValueOf,
          static_cast<jdouble>(decoded.valueAt<double>(row)));
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      auto value = decoded.valueAt<StringView>(row);
      if (kind == TypeKind::VARBINARY) {
        auto bytes = env->NewByteArray(static_cast<jsize>(value.size()));
        if (value.size() > 0) {
          env->SetByteArrayRegion(
              bytes, 0, static_cast<jsize>(value.size()),
              reinterpret_cast<const jbyte*>(value.data()));
        }
        return bytes;
      }
      return newUtf8String(env, callback, value.data(), value.size());
    }
    default:
      throw std::invalid_argument(
          "Unsupported scalar argument kind: " +
          std::to_string(static_cast<int>(kind)));
  }
}

jobject arrayToJava(
    JNIEnv* env,
    const DecodedVector& decoded,
    vector_size_t row) {
  auto base = decoded.base()->as<ArrayVector>();
  const auto baseRow = decoded.index(row);
  const auto offset = base->offsetAt(baseRow);
  const auto size = base->sizeAt(baseRow);
  DecodedVector elements(*base->elements());
  auto result = env->NewFloatArray(size);
  if (!result) throw std::runtime_error("Failed to allocate Java vector");
  std::vector<jfloat> values(size);
  for (vector_size_t i = 0; i < size; ++i) {
    if (elements.isNullAt(offset + i)) {
      throw std::invalid_argument("VECTOR elements must not be null");
    }
    values[i] = elements.valueAt<float>(offset + i);
  }
  if (size > 0) {
    env->SetFloatArrayRegion(result, 0, size, values.data());
  }
  return result;
}

jobject rowToJava(
    JNIEnv* env,
    const CallbackSnapshot& callback,
    const DecodedVector& decoded,
    vector_size_t row) {
  auto base = decoded.base()->as<RowVector>();
  if (base->childrenSize() != kFileFieldCount) {
    throw std::invalid_argument("FILE row must contain exactly 5 fields");
  }
  const auto baseRow = decoded.index(row);
  auto result = env->NewObjectArray(
      static_cast<jsize>(kFileFieldCount), callback.objectClass, nullptr);
  for (size_t field = 0; field < kFileFieldCount; ++field) {
    DecodedVector child(*base->childAt(field));
    if (child.isNullAt(baseRow)) continue;
    jobject value = scalarToJava(
        env, callback, child, baseRow, base->childAt(field)->typeKind());
    env->SetObjectArrayElement(result, static_cast<jsize>(field), value);
    env->DeleteLocalRef(value);
  }
  return result;
}

jobject argumentToJava(
    JNIEnv* env,
    const CallbackSnapshot& callback,
    const DecodedVector& decoded,
    vector_size_t row,
    const TypePtr& type) {
  if (decoded.isNullAt(row)) return nullptr;
  if (type->kind() == TypeKind::ARRAY) {
    return arrayToJava(env, decoded, row);
  }
  if (type->kind() == TypeKind::ROW) {
    return rowToJava(env, callback, decoded, row);
  }
  return scalarToJava(env, callback, decoded, row, type->kind());
}

std::vector<float> javaVector(JNIEnv* env, jobject result) {
  auto array = static_cast<jfloatArray>(result);
  const auto size = env->GetArrayLength(array);
  std::vector<float> values(size);
  if (size > 0) {
    env->GetFloatArrayRegion(array, 0, size, values.data());
  }
  return values;
}

std::string javaString(
    JNIEnv* env,
    const CallbackSnapshot& callback,
    jobject result) {
  jstring charset = env->NewStringUTF("UTF-8");
  auto bytes = static_cast<jbyteArray>(env->CallObjectMethod(
      result, callback.stringGetBytes, charset));
  env->DeleteLocalRef(charset);
  if (env->ExceptionCheck()) {
    throw std::runtime_error(consumeJavaException(env));
  }
  const auto size = env->GetArrayLength(bytes);
  std::string value(size, '\0');
  if (size > 0) {
    env->GetByteArrayRegion(
        bytes, 0, size, reinterpret_cast<jbyte*>(value.data()));
  }
  env->DeleteLocalRef(bytes);
  return value;
}

struct FileResult {
  std::string uri;
  std::string mimeType;
  int64_t size = 0;
  std::string checksum;
  int64_t createdAtMicros = 0;
};

FileResult javaFile(
    JNIEnv* env,
    const CallbackSnapshot& callback,
    jobject result) {
  auto fields = static_cast<jobjectArray>(result);
  if (env->GetArrayLength(fields) != kFileFieldCount) {
    throw std::invalid_argument("Java FILE result must contain 5 fields");
  }
  FileResult file;
  auto uri = env->GetObjectArrayElement(fields, 0);
  auto mime = env->GetObjectArrayElement(fields, 1);
  auto size = env->GetObjectArrayElement(fields, 2);
  auto checksum = env->GetObjectArrayElement(fields, 3);
  auto createdAt = env->GetObjectArrayElement(fields, 4);
  if (!uri || !mime || !size || !checksum || !createdAt) {
    throw std::invalid_argument("Java FILE result fields must not be null");
  }
  file.uri = javaString(env, callback, uri);
  file.mimeType = javaString(env, callback, mime);
  file.size = env->CallLongMethod(size, callback.longValue);
  file.checksum = javaString(env, callback, checksum);
  file.createdAtMicros = env->CallLongMethod(createdAt, callback.longValue);
  env->DeleteLocalRef(uri);
  env->DeleteLocalRef(mime);
  env->DeleteLocalRef(size);
  env->DeleteLocalRef(checksum);
  env->DeleteLocalRef(createdAt);
  return file;
}

void setScalarResult(
    JNIEnv* env,
    const CallbackSnapshot& callback,
    jobject value,
    vector_size_t row,
    const TypePtr& type,
    const VectorPtr& vector) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      vector->asFlatVector<bool>()->set(
          row, env->CallBooleanMethod(value, callback.booleanValue));
      break;
    case TypeKind::INTEGER:
      vector->asFlatVector<int32_t>()->set(
          row, env->CallIntMethod(value, callback.integerValue));
      break;
    case TypeKind::BIGINT:
      vector->asFlatVector<int64_t>()->set(
          row, env->CallLongMethod(value, callback.longValue));
      break;
    case TypeKind::REAL:
      vector->asFlatVector<float>()->set(
          row, env->CallFloatMethod(value, callback.floatValue));
      break;
    case TypeKind::DOUBLE:
      vector->asFlatVector<double>()->set(
          row, env->CallDoubleMethod(value, callback.doubleValue));
      break;
    case TypeKind::VARCHAR: {
      const auto text = javaString(env, callback, value);
      vector->asFlatVector<StringView>()->set(row, StringView(text));
      break;
    }
    case TypeKind::VARBINARY: {
      auto bytes = static_cast<jbyteArray>(value);
      const auto size = env->GetArrayLength(bytes);
      std::string binary(size, '\0');
      if (size > 0) {
        env->GetByteArrayRegion(
            bytes, 0, size, reinterpret_cast<jbyte*>(binary.data()));
      }
      vector->asFlatVector<StringView>()->set(row, StringView(binary));
      break;
    }
    default:
      throw std::invalid_argument(
          "Unsupported scalar result kind: " + type->toString());
  }
}

VectorPtr buildArrayResult(
    const SelectivityVector& rows,
    const TypePtr& type,
    const std::vector<std::vector<float>>& values,
    const std::vector<bool>& nulls,
    facebook::velox::memory::MemoryPool* pool) {
  vector_size_t totalElements = 0;
  for (vector_size_t row = 0; row < rows.end(); ++row) {
    if (!nulls[row]) totalElements += values[row].size();
  }
  auto offsets = AlignedBuffer::allocate<vector_size_t>(rows.end(), pool);
  auto sizes = AlignedBuffer::allocate<vector_size_t>(rows.end(), pool);
  auto nullBuffer = AlignedBuffer::allocate<bool>(rows.end(), pool, bits::kNotNull);
  auto elements = BaseVector::create(REAL(), totalElements, pool);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  auto* rawNulls = nullBuffer->asMutable<uint64_t>();
  auto* flatElements = elements->asFlatVector<float>();
  vector_size_t offset = 0;
  for (vector_size_t row = 0; row < rows.end(); ++row) {
    rawOffsets[row] = offset;
    rawSizes[row] = nulls[row]
        ? 0
        : static_cast<vector_size_t>(values[row].size());
    bits::setNull(rawNulls, row, nulls[row]);
    for (float value : values[row]) flatElements->set(offset++, value);
  }
  return std::make_shared<ArrayVector>(
      pool, type, nullBuffer, rows.end(), offsets, sizes, elements);
}

VectorPtr buildFileResult(
    const SelectivityVector& rows,
    const TypePtr& type,
    const std::vector<FileResult>& files,
    const std::vector<bool>& nulls,
    facebook::velox::memory::MemoryPool* pool) {
  std::vector<VectorPtr> children;
  children.reserve(kFileFieldCount);
  children.push_back(BaseVector::create(VARCHAR(), rows.end(), pool));
  children.push_back(BaseVector::create(VARCHAR(), rows.end(), pool));
  children.push_back(BaseVector::create(BIGINT(), rows.end(), pool));
  children.push_back(BaseVector::create(VARCHAR(), rows.end(), pool));
  children.push_back(BaseVector::create(TIMESTAMP(), rows.end(), pool));
  auto rowNulls = AlignedBuffer::allocate<bool>(rows.end(), pool, bits::kNotNull);
  auto* rawNulls = rowNulls->asMutable<uint64_t>();
  for (vector_size_t row = 0; row < rows.end(); ++row) {
    bits::setNull(rawNulls, row, nulls[row]);
    if (nulls[row]) continue;
    children[0]->asFlatVector<StringView>()->set(row, StringView(files[row].uri));
    children[1]->asFlatVector<StringView>()->set(row, StringView(files[row].mimeType));
    children[2]->asFlatVector<int64_t>()->set(row, files[row].size);
    children[3]->asFlatVector<StringView>()->set(row, StringView(files[row].checksum));
    children[4]->asFlatVector<Timestamp>()->set(
        row, Timestamp::fromMicros(files[row].createdAtMicros));
  }
  return std::make_shared<RowVector>(
      pool, type, rowNulls, rows.end(), std::move(children));
}

std::string decodedString(
    const DecodedVector& decoded, vector_size_t row, const char* field) {
  if (decoded.isNullAt(row) ||
      decoded.base()->typeKind() != TypeKind::VARCHAR) {
    throw std::invalid_argument(
        std::string("JVM callback ") + field + " must be VARCHAR");
  }
  const auto value = decoded.valueAt<StringView>(row);
  return std::string(value.data(), value.size());
}

uint64_t positiveUint64(const std::string& value, const char* field) {
  try {
    size_t consumed = 0;
    const uint64_t parsed = std::stoull(value, &consumed);
    if (parsed == 0 || consumed != value.size()) throw std::invalid_argument(field);
    return parsed;
  } catch (const std::exception&) {
    throw std::invalid_argument(
        std::string("JVM callback ") + field + " must be positive");
  }
}

struct JvmCallContext final {
  std::string callback_context;
  int64_t executor_handle = 0;
};

class JvmScalarFunction final : public exec::VectorFunction {
 public:
  JvmScalarFunction(
      std::string name,
      TypePtr returnType,
      JvmFunctionCallbackOwner callbacks)
      : name_(std::move(name)),
        returnType_(std::move(returnType)),
        callbacks_(std::move(callbacks)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr&,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    if (!callbacks_) {
      throw std::runtime_error("JVM scalar function has no bound callback generation");
    }
    ScopedJniEnv scopedEnv;
    JNIEnv* env = scopedEnv.get();
    if (env->PushLocalFrame(256) < 0) {
      throw std::runtime_error("Failed to allocate JNI local frame");
    }
    try {
      const auto callback = callbacks_->snapshot(env);
      exec::DecodedArgs decoded(rows, args, context);
      std::vector<vector_size_t> requestedRows;
      requestedRows.reserve(rows.countSelected());
      rows.applyToSelected([&](vector_size_t row) { requestedRows.push_back(row); });
      const uint32_t expectedVectorDimension = requestedRows.empty()
          ? 0
          : exactVectorDimension(decoded, args, requestedRows.front());
      const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();
      auto batch = env->NewObjectArray(
          rows.countSelected(), callback.objectArrayClass, nullptr);
      std::vector<vector_size_t> selectedRows;
      selectedRows.reserve(rows.countSelected());
      jsize batchRow = 0;
      for (vector_size_t row : requestedRows) {
        auto arguments = env->NewObjectArray(
            static_cast<jsize>(args.size()), callback.objectClass, nullptr);
        for (size_t argument = 0; argument < args.size(); ++argument) {
          jobject value = argumentToJava(
              env, callback, *decoded.at(argument), row, args[argument]->type());
          if (value) {
            env->SetObjectArrayElement(
                arguments, static_cast<jsize>(argument), value);
            env->DeleteLocalRef(value);
          }
        }
        env->SetObjectArrayElement(batch, batchRow++, arguments);
        env->DeleteLocalRef(arguments);
        selectedRows.push_back(row);
      }
      const auto callbackContext = queryConfig.get<std::string>(
          "pangrova.execution_callback_context", "");
      const auto executorHandle = positiveUint64(queryConfig.get<std::string>(
          "pangrova.executor_handle", ""), "executor handle");
      if (executorHandle > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        throw std::invalid_argument("JVM callback executor handle exceeds int64 range");
      }
      jobjectArray javaResults = nullptr;
      if (batchRow > 0) {
        javaResults = invokeJava(env, callback, batch, JvmCallContext{
            callbackContext, static_cast<int64_t>(executorHandle)});
      }
      if (batchRow > 0 &&
          (!javaResults || env->GetArrayLength(javaResults) != batchRow)) {
        throw std::runtime_error(
            "JVM scalar function '" + name_ +
            "' returned an invalid batch cardinality");
      }
      writeResults(
          env, callback, javaResults, selectedRows, rows, context, result,
          expectedVectorDimension);
      env->PopLocalFrame(nullptr);
    } catch (...) {
      env->PopLocalFrame(nullptr);
      throw;
    }
  }

 private:
  uint32_t exactVectorDimension(
      const exec::DecodedArgs& decoded,
      const std::vector<VectorPtr>& args,
      vector_size_t row) const {
    if (name_ != "ai_embed") return 0;
    if (args.size() < kGovernedHiddenArguments) {
      throw std::invalid_argument(
          "AI_EMBED is missing its exact model output contract");
    }
    const size_t offset = args.size() - kGovernedHiddenArguments;
    const uint64_t dimension = positiveUint64(decodedString(
        *decoded.at(offset + kVectorDimensionOffset), row,
        "vector dimension"), "vector dimension");
    if (dimension > 65'535) {
      throw std::invalid_argument("AI_EMBED vector dimension exceeds 65535");
    }
    return static_cast<uint32_t>(dimension);
  }

  jobjectArray invokeJava(
      JNIEnv* env,
      const CallbackSnapshot& callback,
      jobjectArray batch,
      const JvmCallContext& call) const {
    jstring function = newUtf8String(env, callback, name_.data(), name_.size());
    if (call.callback_context.empty() || call.callback_context.size() > 1'048'576) {
      throw std::runtime_error("JVM callback is missing bounded canonical task authority");
    }
    auto authority = env->NewByteArray(static_cast<jsize>(call.callback_context.size()));
    if (!authority) throw std::runtime_error("Cannot allocate JVM callback authority bytes");
    env->SetByteArrayRegion(authority, 0, static_cast<jsize>(call.callback_context.size()),
        reinterpret_cast<const jbyte*>(call.callback_context.data()));
    if (env->ExceptionCheck()) throw std::runtime_error("Cannot copy JVM callback authority bytes");
    auto results = static_cast<jobjectArray>(env->CallObjectMethod(
        callback.callback, callback.evaluateBatch, function, batch, authority));
    if (env->ExceptionCheck()) {
      auto failure = env->ExceptionOccurred();
      env->ExceptionClear();
      auto encoded = static_cast<jbyteArray>(env->CallObjectMethod(
          callback.callback, callback.describeAiFailure, failure));
      if (env->ExceptionCheck()) {
        throw std::runtime_error("JVM AI failure projection failed: " +
                                 consumeJavaException(env));
      }
      if (encoded) {
        const jsize length = env->GetArrayLength(encoded);
        if (length <= 0 || length > 65'536) {
          throw std::runtime_error("JVM AI failure projection has invalid size");
        }
        std::string bytes(static_cast<size_t>(length), '\0');
        env->GetByteArrayRegion(encoded, 0, length,
                                reinterpret_cast<jbyte*>(bytes.data()));
        if (env->ExceptionCheck()) {
          throw std::runtime_error("JVM AI failure projection could not be read: " +
                                   consumeJavaException(env));
        }
        pangrova::protocol::v2::ExecutionError error;
        if (!error.ParseFromString(bytes) ||
            !pangrova::protocol::v2::ErrorClass_IsValid(error.error_class()) ||
            error.error_class() == pangrova::protocol::v2::ERROR_CLASS_UNSPECIFIED ||
            error.code().empty() || error.message().empty() ||
            error.failed_component().empty() ||
            !error.GetReflection()->GetUnknownFields(error).empty()) {
          throw std::runtime_error("JVM AI failure projection is not canonical");
        }
        pangrova::jni::HandleRegistry::instance()
            .acquire(call.executor_handle)->recordExecutionError(error);
      }
      env->Throw(failure);
      throw std::runtime_error(
          "JVM scalar function '" + name_ + "' failed: " +
          consumeJavaException(env));
    }
    return results;
  }

  void writeResults(
      JNIEnv* env,
      const CallbackSnapshot& callback,
      jobjectArray javaResults,
      const std::vector<vector_size_t>& selectedRows,
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      VectorPtr& result,
      uint32_t expectedVectorDimension) const {
    std::vector<bool> nulls(rows.end(), true);
    if (returnType_->kind() == TypeKind::ARRAY) {
      std::vector<std::vector<float>> values(rows.end());
      for (jsize i = 0; i < static_cast<jsize>(selectedRows.size()); ++i) {
        jobject value = env->GetObjectArrayElement(javaResults, i);
        if (value) {
          nulls[selectedRows[i]] = false;
          values[selectedRows[i]] = javaVector(env, value);
          if (expectedVectorDimension != 0 &&
              values[selectedRows[i]].size() != expectedVectorDimension) {
            throw std::runtime_error(
                "AI_EMBED returned dimension " +
                std::to_string(values[selectedRows[i]].size()) +
                "; exact model contract requires " +
                std::to_string(expectedVectorDimension));
          }
          env->DeleteLocalRef(value);
        }
      }
      auto local = buildArrayResult(rows, returnType_, values, nulls, context.pool());
      context.moveOrCopyResult(local, rows, result);
      return;
    }
    if (returnType_->kind() == TypeKind::ROW) {
      std::vector<FileResult> files(rows.end());
      for (jsize i = 0; i < static_cast<jsize>(selectedRows.size()); ++i) {
        jobject value = env->GetObjectArrayElement(javaResults, i);
        if (value) {
          nulls[selectedRows[i]] = false;
          files[selectedRows[i]] = javaFile(env, callback, value);
          env->DeleteLocalRef(value);
        }
      }
      auto local = buildFileResult(rows, returnType_, files, nulls, context.pool());
      context.moveOrCopyResult(local, rows, result);
      return;
    }
    auto local = BaseVector::create(returnType_, rows.end(), context.pool());
    for (jsize i = 0; i < static_cast<jsize>(selectedRows.size()); ++i) {
      const auto row = selectedRows[i];
      jobject value = env->GetObjectArrayElement(javaResults, i);
      if (!value) {
        local->setNull(row, true);
      } else {
        setScalarResult(env, callback, value, row, returnType_, local);
        env->DeleteLocalRef(value);
      }
    }
    if (env->ExceptionCheck()) {
      throw std::runtime_error(consumeJavaException(env));
    }
    context.moveOrCopyResult(local, rows, result);
  }

  std::string name_;
  TypePtr returnType_;
  JvmFunctionCallbackOwner callbacks_;
};

}  // namespace

JvmFunctionCallbackOwner prepareJvmFunctionCallbacks(JNIEnv* env, jobject callback) {
  auto owner = std::make_shared<JvmFunctionCallbacks>();
  owner->install(env, callback);
  return owner;
}

void publishJvmFunctionCallbacks(JvmFunctionCallbackOwner owner) {
  if (!owner) {
    throw std::invalid_argument("Cannot publish an empty JVM callback generation");
  }
  // Reference release (which may attach to JNI) must happen outside the mutex.
  JvmFunctionCallbackOwner previous;
  {
    std::lock_guard lock(callbackPublicationMutex);
    previous = std::exchange(activeCallbacks, std::move(owner));
  }
}

void clearJvmFunctionCallbacks(JNIEnv* /*env*/) {
  JvmFunctionCallbackOwner previous;
  {
    std::lock_guard lock(callbackPublicationMutex);
    previous = std::exchange(activeCallbacks, {});
  }
}

void bindJvmFunctionCallbacks(ScalarUdfSpec& spec, JvmFunctionCallbackOwner owner) {
  if (!owner) {
    throw std::invalid_argument("Cannot bind an empty JVM callback generation");
  }
  spec.vectorFunctionFactory =
      [name = spec.name, output = spec.outputType, owner = std::move(owner)](
          const std::string&, const std::vector<exec::VectorFunctionArg>&) {
        return std::make_unique<JvmScalarFunction>(name, output, owner);
      };
}

ScalarUdfSpec makeJvmScalarFunctionSpec(
    const protocol::v2::NativeFunctionDescriptor& descriptor) {
  if (!descriptor.has_default_null_behavior() ||
      descriptor.GetReflection()->GetUnknownFields(descriptor).field_count() != 0) {
    throw std::invalid_argument(
        "Function descriptor requires explicit known NULL-input metadata");
  }
  if (descriptor.function_kind() != protocol::v2::NATIVE_FUNCTION_KIND_SCALAR) {
    throw std::invalid_argument(
        "JVM aggregate function registration is not supported: " + descriptor.name());
  }
  const auto requireCanonicalString = [](const protocol::v2::LogicalType& type) {
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    type.GetReflection()->ListFields(type, &fields);
    if (type.GetReflection()->GetUnknownFields(type).field_count() != 0 ||
        fields.size() != 1 ||
        fields.front()->number() != protocol::v2::LogicalType::kCanonicalSqlFieldNumber) {
      throw std::invalid_argument(
          "Function descriptor LogicalType must contain exactly its canonical SQL declaration");
    }
  };
  requireCanonicalString(descriptor.return_type());
  for (const auto& argument : descriptor.argument_types()) {
    requireCanonicalString(argument);
  }
  const auto declared = pangrova::capability::generated::findRuntimeCallbackSignatures(exec::sanitizeName(descriptor.name()));
  if (!declared.empty()) {
    const bool matches = std::any_of(declared.begin(), declared.end(), [&](const auto& signature) {
      if (descriptor.name() != signature.name ||
          descriptor.return_type().canonical_sql() != signature.returnType ||
          descriptor.deterministic() != signature.deterministic ||
          descriptor.default_null_behavior() != signature.defaultNullBehavior ||
          static_cast<size_t>(descriptor.argument_types_size()) != signature.arguments.size()) {
        return false;
      }
      for (int ordinal = 0; ordinal < descriptor.argument_types_size(); ++ordinal) {
        if (descriptor.argument_types(ordinal).canonical_sql() != signature.arguments[static_cast<size_t>(ordinal)]) {
          return false;
        }
      }
      return true;
    });
    if (!matches) {
      throw std::invalid_argument(
          "Function descriptor differs from its sole canonical callback declaration: " + descriptor.name());
    }
  }
  std::vector<std::string> argumentTypes;
  argumentTypes.reserve(descriptor.argument_types_size());
  for (const auto& argument : descriptor.argument_types()) {
    argumentTypes.push_back(argument.canonical_sql());
  }
  return makeJvmScalarFunctionSpec(descriptor.name(), argumentTypes,
      descriptor.return_type().canonical_sql(), descriptor.deterministic(),
      descriptor.default_null_behavior());
}

ScalarUdfSpec makeJvmScalarFunctionSpec(
    const std::string& name,
    const std::vector<std::string>& argumentTypes,
    const std::string& returnType,
    bool deterministic,
    bool defaultNullBehavior) {
  ScalarUdfSpec spec;
  spec.name = name;
  spec.inputTypes.reserve(argumentTypes.size());
  for (const auto& argumentType : argumentTypes) {
    spec.inputTypes.push_back(parseType(argumentType));
  }
  spec.outputType = parseType(returnType);
  spec.stateful = false;
  spec.deterministic = deterministic;
  spec.defaultNullBehavior = defaultNullBehavior;
  spec.vectorFunctionFactory =
      [functionName = spec.name,
       output = spec.outputType](
          const std::string&,
          const std::vector<exec::VectorFunctionArg>&) {
        return std::make_unique<JvmScalarFunction>(
            functionName, output, nullptr);
      };
  return spec;
}

}  // namespace pangrova::udf
