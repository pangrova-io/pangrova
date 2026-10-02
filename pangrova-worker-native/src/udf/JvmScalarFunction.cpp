// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "udf/JvmScalarFunction.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <openssl/evp.h>

#include "cache/SemanticResultCache.h"
#include "jni/bridge/JniBridge.h"
#include "observability/PangrovaLog.h"
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
constexpr size_t kReuseEnabledOffset = governedOffset("reuse_enabled");
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

class CallbackRegistry {
 public:
  static CallbackRegistry& instance() {
    static CallbackRegistry registry;
    return registry;
  }

  void install(JNIEnv* env, jobject callback) {
    if (!callback) {
      throw std::invalid_argument("JVM function callback must not be null");
    }
    std::unique_lock lock(mutex_);
    clearLocked(env);
    try {
      callback_ = env->NewGlobalRef(callback);
      callbackClass_ = globalClass(env, "org/pangrova/worker/jni/NativeFunctionBridge");
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

  void clear(JNIEnv* env) {
    std::unique_lock lock(mutex_);
    clearLocked(env);
  }

  CallbackSnapshot snapshot(JNIEnv* env) const {
    std::shared_lock lock(mutex_);
    if (!callback_ || !evaluateBatch_) {
      throw std::runtime_error("JVM function callback is not installed");
    }
    CallbackSnapshot result;
    result.callback = env->NewLocalRef(callback_);
    if (!result.callback) {
      throw std::runtime_error("Failed to retain JVM function callback");
    }
    result.evaluateBatch = evaluateBatch_;
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

  mutable std::shared_mutex mutex_;
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
        std::string("Semantic cache ") + field + " must be VARCHAR");
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
        std::string("Semantic cache ") + field + " must be positive");
  }
}

double reuseThreshold(const std::string& value) {
  try {
    size_t consumed = 0;
    const double parsed = std::stod(value, &consumed);
    if (consumed != value.size() || !std::isfinite(parsed) ||
        parsed < 0.90 || parsed > 1.0) {
      throw std::invalid_argument("threshold");
    }
    return parsed;
  } catch (const std::exception&) {
    throw std::invalid_argument("Semantic cache threshold is invalid");
  }
}

void appendCanonical(std::string& target, std::string_view value) {
  const uint64_t size = value.size();
  target.append(reinterpret_cast<const char*>(&size), sizeof(size));
  target.append(value);
}

std::string sha256Hex(const std::string& value) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int size = 0;
  if (EVP_Digest(
          value.data(), value.size(), digest, &size, EVP_sha256(), nullptr) != 1 ||
      size != 32) {
    throw std::runtime_error("Semantic cache key SHA-256 failed");
  }
  constexpr char digits[] = "0123456789abcdef";
  std::string result(size * 2, '\0');
  for (unsigned int index = 0; index < size; ++index) {
    result[index * 2] = digits[digest[index] >> 4U];
    result[index * 2 + 1] = digits[digest[index] & 0x0fU];
  }
  return result;
}

void appendArgument(
    std::string& target,
    const DecodedVector& decoded,
    vector_size_t row,
    const TypePtr& type) {
  appendCanonical(target, type->toString());
  if (decoded.isNullAt(row)) {
    appendCanonical(target, "<null>");
    return;
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      appendCanonical(target, decoded.valueAt<bool>(row) ? "1" : "0");
      return;
    case TypeKind::TINYINT:
      appendCanonical(target, std::to_string(decoded.valueAt<int8_t>(row)));
      return;
    case TypeKind::SMALLINT:
      appendCanonical(target, std::to_string(decoded.valueAt<int16_t>(row)));
      return;
    case TypeKind::INTEGER:
      appendCanonical(target, std::to_string(decoded.valueAt<int32_t>(row)));
      return;
    case TypeKind::BIGINT:
      appendCanonical(target, std::to_string(decoded.valueAt<int64_t>(row)));
      return;
    case TypeKind::REAL:
      appendCanonical(target, std::to_string(decoded.valueAt<float>(row)));
      return;
    case TypeKind::DOUBLE:
      appendCanonical(target, std::to_string(decoded.valueAt<double>(row)));
      return;
    case TypeKind::VARCHAR: {
      const auto value = decoded.valueAt<StringView>(row);
      appendCanonical(target, std::string_view(value.data(), value.size()));
      return;
    }
    default:
      throw std::invalid_argument(
          "Semantic cache parameter type is not canonically encodable: " +
          type->toString());
  }
}

struct CacheBatchContext final {
  cache::ReuseIdentity identity;
  cache::ReusePolicy policy;
  size_t public_arguments{0};
};

struct JvmCallContext final {
  std::string callback_context;
};

class JvmScalarFunction final : public exec::VectorFunction {
 public:
  JvmScalarFunction(
      std::string name,
      TypePtr returnType)
      : name_(std::move(name)),
        returnType_(std::move(returnType)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr&,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    ScopedJniEnv scopedEnv;
    JNIEnv* env = scopedEnv.get();
    if (env->PushLocalFrame(256) < 0) {
      throw std::runtime_error("Failed to allocate JNI local frame");
    }
    try {
      const auto callback = CallbackRegistry::instance().snapshot(env);
      exec::DecodedArgs decoded(rows, args, context);
      std::vector<vector_size_t> requestedRows;
      requestedRows.reserve(rows.countSelected());
      rows.applyToSelected([&](vector_size_t row) { requestedRows.push_back(row); });
      const uint32_t expectedVectorDimension = requestedRows.empty()
          ? 0
          : exactVectorDimension(decoded, args, requestedRows.front());
      const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();
      const auto taskId = queryConfig.get<std::string>("pangrova.task_id", "");
      auto cacheContext = requestedRows.empty()
          ? std::optional<CacheBatchContext>{}
          : buildCacheContext(
                decoded, args, requestedRows.front(), queryConfig);
      std::vector<std::optional<cache::CachedValue>> cacheValues(rows.end());
      std::vector<std::vector<float>> requestKeys(rows.end());
      std::vector<cache::ReuseIdentity> identities(rows.end());

      auto batch = env->NewObjectArray(
          rows.countSelected(), callback.objectArrayClass, nullptr);
      std::vector<vector_size_t> selectedRows;
      selectedRows.reserve(rows.countSelected());
      jsize batchRow = 0;
      for (vector_size_t row : requestedRows) {
        if (cacheContext) {
          identities[row] = identityForRow(
              *cacheContext, decoded, args, row);
          requestKeys[row] = cache::SemanticResultCache::requestKey(
              decodedString(*decoded.at(1), row, "request text"),
              cacheContext->policy.key_dimension);
          cacheValues[row] = cache::SemanticResultCache::instance().lookup(
              identities[row], requestKeys[row], cacheContext->policy, taskId);
          if (cacheValues[row].has_value()) continue;
        }
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
      jobjectArray javaResults = nullptr;
      if (batchRow > 0) {
        javaResults = invokeJava(env, callback, batch, JvmCallContext{
            callbackContext});
      }
      if (batchRow > 0 &&
          (!javaResults || env->GetArrayLength(javaResults) != batchRow)) {
        throw std::runtime_error(
            "JVM scalar function '" + name_ +
            "' returned an invalid batch cardinality");
      }
      if (cacheContext) {
        cacheMissResults(
            env, callback, javaResults, selectedRows, identities, requestKeys,
            cacheValues, cacheContext->policy, taskId);
        writeCacheResults(cacheValues, rows, context, result);
        PANGROVA_LOG_INFO << "semantic_cache_batch"
                          << " task_id=" << taskId
                          << " function=" << name_
                          << " requested=" << requestedRows.size()
                          << " provider_rows=" << selectedRows.size();
      } else {
        writeResults(
            env, callback, javaResults, selectedRows, rows, context, result,
            expectedVectorDimension);
      }
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

  template <typename QueryConfig>
  std::optional<CacheBatchContext> buildCacheContext(
      const exec::DecodedArgs& decoded,
      const std::vector<VectorPtr>& args,
      vector_size_t row,
      const QueryConfig& query_config) const {
    if (pangrova::capability::generated::governedLayoutForImplementation(name_) != "AI_MODEL") return std::nullopt;
    if (args.size() < kGovernedHiddenArguments) throw std::invalid_argument("Governed callback hidden layout is incomplete");
    const size_t offset = args.size() - kGovernedHiddenArguments;
    const std::string enabled = decodedString(
        *decoded.at(offset + kReuseEnabledOffset), row, "enabled flag");
    if (enabled == "false") return std::nullopt;
    if (enabled != "true" || returnType_->kind() != TypeKind::VARCHAR ||
        offset < 2) {
      throw std::invalid_argument(
          "Semantic cache binding is enabled for an unsupported invocation");
    }
    const std::string require_complete = decodedString(
        *decoded.at(offset + governedOffset("reuse_require_complete")), row, "complete-only flag");
    if (require_complete != "true") {
      throw std::invalid_argument("Semantic cache requires complete-only writes");
    }
    CacheBatchContext result;
    result.public_arguments = offset;
    result.policy.ttl_ms = positiveUint64(decodedString(
        *decoded.at(offset + governedOffset("reuse_ttl_ms")), row, "ttl_ms"), "ttl_ms");
    result.policy.max_entries = positiveUint64(decodedString(
        *decoded.at(offset + governedOffset("reuse_max_entries")), row, "max_entries"), "max_entries");
    result.policy.max_bytes = positiveUint64(decodedString(
        *decoded.at(offset + governedOffset("reuse_max_bytes")), row, "max_bytes"), "max_bytes");
    result.policy.similarity_threshold = reuseThreshold(decodedString(
        *decoded.at(offset + governedOffset("reuse_similarity_threshold")), row, "similarity_threshold"));
    result.policy.key_dimension = static_cast<uint32_t>(positiveUint64(
        decodedString(*decoded.at(offset + governedOffset("reuse_key_dimension")), row, "key_dimension"),
        "key_dimension"));
    result.identity.tenant_id = query_config.template get<std::string>(
        "pangrova.tenant_id", "");
    result.identity.principal = query_config.template get<std::string>(
        "pangrova.principal", "");
    result.identity.policy_context_hash = query_config.template get<std::string>(
        "pangrova.policy_context_hash", "");
    result.identity.dependency_scope_hash =
        query_config.template get<std::string>(
            "pangrova.dependency_scope_hash", "");
    result.identity.dependency_version_hash =
        query_config.template get<std::string>(
            "pangrova.dependency_version_hash", "");
    if (result.identity.tenant_id.empty() || result.identity.principal.empty() ||
        result.identity.policy_context_hash.empty() ||
        result.identity.dependency_scope_hash.empty() ||
        result.identity.dependency_version_hash.empty()) {
      throw std::invalid_argument(
          "Semantic cache security or dependency identity is incomplete");
    }
    std::string model;
    for (const auto name : {"model_id", "model_version", "model_signature"}) {
      appendCanonical(model, decodedString(
          *decoded.at(offset + governedOffset(name)), row, "model identity"));
    }
    std::string capability;
    for (const auto name : {"capability_id", "capability_version", "capability_signature"}) {
      appendCanonical(capability, decodedString(
          *decoded.at(offset + governedOffset(name)), row, "capability identity"));
    }
    result.identity.model_identity = sha256Hex(model);
    result.identity.capability_identity = sha256Hex(capability);
    result.identity.function_name = name_;
    result.identity.result_type = returnType_->toString();
    return result;
  }

  cache::ReuseIdentity identityForRow(
      const CacheBatchContext& base,
      const exec::DecodedArgs& decoded,
      const std::vector<VectorPtr>& args,
      vector_size_t row) const {
    cache::ReuseIdentity identity = base.identity;
    std::string parameters;
    for (size_t index = 0; index < base.public_arguments; ++index) {
      if (index == 1) continue;
      appendArgument(parameters, *decoded.at(index), row, args[index]->type());
    }
    identity.parameter_fingerprint = sha256Hex(parameters);
    return identity;
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
      throw std::runtime_error(
          "JVM scalar function '" + name_ + "' failed: " +
          consumeJavaException(env));
    }
    return results;
  }

  void cacheMissResults(
      JNIEnv* env,
      const CallbackSnapshot& callback,
      jobjectArray java_results,
      const std::vector<vector_size_t>& missed_rows,
      const std::vector<cache::ReuseIdentity>& identities,
      const std::vector<std::vector<float>>& request_keys,
      std::vector<std::optional<cache::CachedValue>>& values,
      const cache::ReusePolicy& policy,
      const std::string& task_id) const {
    for (jsize index = 0; index < static_cast<jsize>(missed_rows.size()); ++index) {
      const vector_size_t row = missed_rows[index];
      jobject result = env->GetObjectArrayElement(java_results, index);
      if (!result) {
        throw std::runtime_error(
            "Semantic cache complete-only invocation returned a null result");
      }
      cache::CachedValue value = javaString(env, callback, result);
      env->DeleteLocalRef(result);
      const bool admitted = cache::SemanticResultCache::instance().put(
          identities[row], request_keys[row], value, policy, task_id);
      if (!admitted) {
        PANGROVA_LOG_WARN << "semantic_cache_admission_rejected"
                          << " task_id=" << task_id
                          << " tenant_id=" << identities[row].tenant_id
                          << " function=" << name_;
      }
      values[row] = std::move(value);
    }
  }

  void writeCacheResults(
      const std::vector<std::optional<cache::CachedValue>>& values,
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    auto local = BaseVector::create(returnType_, rows.end(), context.pool());
    rows.applyToSelected([&](vector_size_t row) {
      if (!values[row].has_value() ||
          !std::holds_alternative<std::string>(*values[row])) {
        throw std::runtime_error(
            "Semantic cache did not produce a complete VARCHAR result");
      }
      const auto& value = std::get<std::string>(*values[row]);
      local->asFlatVector<StringView>()->set(row, StringView(value));
    });
    context.moveOrCopyResult(local, rows, result);
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
};

}  // namespace

void installJvmFunctionCallbacks(JNIEnv* env, jobject callback) {
  CallbackRegistry::instance().install(env, callback);
}

void clearJvmFunctionCallbacks(JNIEnv* env) {
  CallbackRegistry::instance().clear(env);
}

ScalarUdfSpec makeJvmScalarFunctionSpec(
    const std::string& name,
    const std::vector<std::string>& argumentTypes,
    const std::string& returnType) {
  ScalarUdfSpec spec;
  spec.name = name;
  spec.inputTypes.reserve(argumentTypes.size());
  for (const auto& argumentType : argumentTypes) {
    spec.inputTypes.push_back(parseType(argumentType));
  }
  spec.outputType = parseType(returnType);
  spec.stateful = false;
  spec.vectorFunctionFactory =
      [functionName = spec.name,
       output = spec.outputType](
          const std::string&,
          const std::vector<exec::VectorFunctionArg>&) {
        return std::make_unique<JvmScalarFunction>(
            functionName, output);
      };
  return spec;
}

}  // namespace pangrova::udf
