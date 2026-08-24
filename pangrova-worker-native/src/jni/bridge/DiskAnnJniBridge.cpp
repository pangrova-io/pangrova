// DiskAnnJniBridge: JNI entry points for DiskAnnEngine.java native methods.
//
// Bridges Java DiskANN build/search requests to the C++ VamanaIndex
// implementation. Uses JniLocalFrameGuard for local reference management
// and JNI_CATCH_AND_THROW for exception propagation.
//
// Placement: jni/bridge/ — JNI bridge layer.
#include "jni/bridge/JniBridge.h"

#include <cstdint>
#include <string>
#include <vector>

#include "vector/diskann/DiskAnnBuilder.h"
#include "vector/diskann/DiskAnnIndexCache.h"
#include "vector/diskann/DiskAnnSearcher.h"
#include "vector/diskann/VamanaGraph.h"

#define JNI_CATCH_AND_THROW(env)                                       \
  catch (const std::exception& e) {                                    \
    pangrova::jni::throwNativeBridgeException(env, e.what());            \
  }                                                                    \
  catch (...) {                                                        \
    pangrova::jni::throwNativeBridgeException(env, "Unknown C++ error"); \
  }

namespace {

// Convert a Java float[][] (jobjectArray of jfloatArray) to a flat float buffer.
std::vector<float> flattenVectorArray(
    JNIEnv* env, jobjectArray vectors, int numVectors, int dimension) {
  std::vector<float> flat(static_cast<size_t>(numVectors) * dimension);
  for (int i = 0; i < numVectors; ++i) {
    auto row = static_cast<jfloatArray>(env->GetObjectArrayElement(vectors, i));
    if (env->ExceptionCheck()) return flat;
    if (!row) continue;
    env->GetFloatArrayRegion(row, 0, dimension, flat.data() + i * dimension);
    if (env->ExceptionCheck()) { env->DeleteLocalRef(row); return flat; }
    env->DeleteLocalRef(row);
  }
  return flat;
}

// Convert a jstring to std::string.
std::string jstringToStd(JNIEnv* env, jstring jstr) {
  if (!jstr) return "";
  const char* chars = env->GetStringUTFChars(jstr, nullptr);
  if (!chars) return {};
  std::string result(chars);
  env->ReleaseStringUTFChars(jstr, chars);
  return result;
}

}  // namespace

extern "C" {

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_ai_vector_DiskAnnEngine_nativeBuildIndex(
    JNIEnv* env, jclass /*clazz*/,
    jobjectArray vectors, jint dimension, jstring metric,
    jint maxDegree, jint buildListSize, jint pqSubvectors,
    jstring outputDirectory) {
  try {
    pangrova::jni::JniLocalFrameGuard frame(env, 32);
    if (!frame.valid()) {
      pangrova::jni::throwNativeBridgeException(env, "PushLocalFrame failed");
      return nullptr;
    }

    int numVectors = env->GetArrayLength(vectors);
    auto flatVectors = flattenVectorArray(env, vectors, numVectors, dimension);
    std::string outputDir = jstringToStd(env, outputDirectory);

    pangrova::vector::diskann::VamanaConfig config;
    config.maxDegree = maxDegree;
    config.searchListSize = buildListSize;
    config.pqSubvectors = pqSubvectors;
    config.dimension = dimension;
    config.numVectors = numVectors;

    pangrova::vector::diskann::DiskAnnBuilder builder(config);
    auto stats = builder.buildFromVectors(
        flatVectors.data(), numVectors, dimension, outputDir);

    // Encode result: [vectorCount:8B][graphSizeBytes:4B][pqCodeSizeBytes:4B]
    constexpr int RESULT_SIZE = 16;
    uint8_t resultBuf[RESULT_SIZE];
    auto* p = resultBuf;
    std::memcpy(p, &stats.numVectors, 8); p += 8;
    int32_t graphSize = static_cast<int32_t>(stats.graphSizeBytes);
    std::memcpy(p, &graphSize, 4); p += 4;
    int32_t pqSize = static_cast<int32_t>(stats.pqCodeSizeBytes);
    std::memcpy(p, &pqSize, 4);

    jbyteArray result = env->NewByteArray(RESULT_SIZE);
    env->SetByteArrayRegion(result, 0, RESULT_SIZE,
                            reinterpret_cast<const jbyte*>(resultBuf));
    return static_cast<jbyteArray>(frame.popWithResult(result));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_ai_vector_DiskAnnEngine_nativeSearch(
    JNIEnv* env, jclass /*clazz*/,
    jstring indexPath, jfloatArray queryVector, jint topK, jint searchListSize) {
  try {
    pangrova::jni::JniLocalFrameGuard frame(env, 16);
    if (!frame.valid()) {
      pangrova::jni::throwNativeBridgeException(env, "PushLocalFrame failed");
      return nullptr;
    }

    std::string path = jstringToStd(env, indexPath);
    int queryDim = env->GetArrayLength(queryVector);
    std::vector<float> query(queryDim);
    env->GetFloatArrayRegion(queryVector, 0, queryDim, query.data());

    // Get cached VamanaIndex (LRU cache eliminates repeated loadFromDisk).
    auto index = pangrova::vector::diskann::DiskAnnIndexCache::instance()
        .getOrLoad(path, queryDim);
    if (!index) {
      pangrova::jni::throwNativeBridgeException(
          env, ("Failed to load DiskANN index from: " + path).c_str());
      return nullptr;
    }

    // Use DiskAnnSearcher with full-precision reranking from SSD.
    pangrova::vector::diskann::DiskAnnSearcher searcher(
        *index, [&](int64_t nodeId, float* buf, int64_t dim) {
          return index->readVector(nodeId, buf, dim);
        });
    pangrova::vector::diskann::DiskAnnSearchConfig searchConfig;
    searchConfig.topK = topK;
    searchConfig.beamWidth = searchListSize;
    searchConfig.rerankMultiplier = 10;
    auto results = searcher.search(query.data(), searchConfig);

    // Encode results: [count:4B] [rowId:8B, distance:8B]*N
    int count = static_cast<int>(results.size());
    int resultSize = 4 + count * 16;
    std::vector<uint8_t> resultBuf(resultSize);
    auto* p = resultBuf.data();
    std::memcpy(p, &count, 4); p += 4;
    for (const auto& r : results) {
      std::memcpy(p, &r.nodeId, 8); p += 8;
      double dist = static_cast<double>(r.distance);
      std::memcpy(p, &dist, 8); p += 8;
    }

    jbyteArray result = env->NewByteArray(resultSize);
    env->SetByteArrayRegion(result, 0, resultSize,
                            reinterpret_cast<const jbyte*>(resultBuf.data()));
    return static_cast<jbyteArray>(frame.popWithResult(result));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_org_pangrova_worker_ai_vector_DiskAnnEngine_nativeSearchFiltered(
    JNIEnv* env, jclass /*clazz*/,
    jstring indexPath, jfloatArray queryVector, jint topK, jint searchListSize,
    jbyteArray preFilterBitset) {
  try {
    pangrova::jni::JniLocalFrameGuard frame(env, 16);
    if (!frame.valid()) {
      pangrova::jni::throwNativeBridgeException(env, "PushLocalFrame failed");
      return nullptr;
    }

    std::string path = jstringToStd(env, indexPath);
    int queryDim = env->GetArrayLength(queryVector);
    std::vector<float> query(queryDim);
    env->GetFloatArrayRegion(queryVector, 0, queryDim, query.data());

    // Get cached VamanaIndex.
    auto index = pangrova::vector::diskann::DiskAnnIndexCache::instance()
        .getOrLoad(path, queryDim);
    if (!index) {
      pangrova::jni::throwNativeBridgeException(
          env, ("Failed to load DiskANN index from: " + path).c_str());
      return nullptr;
    }

    // Configure search with pre-filter bitset.
    pangrova::vector::diskann::DiskAnnSearchConfig searchConfig;
    searchConfig.topK = topK;
    searchConfig.beamWidth = searchListSize;
    searchConfig.rerankMultiplier = 10;

    // Extract bitset from Java byte array.
    std::vector<uint8_t> bitsetBuf;
    if (preFilterBitset) {
      jsize bitsetLen = env->GetArrayLength(preFilterBitset);
      if (bitsetLen > 0) {
        bitsetBuf.resize(static_cast<size_t>(bitsetLen));
        env->GetByteArrayRegion(preFilterBitset, 0, bitsetLen,
                                reinterpret_cast<jbyte*>(bitsetBuf.data()));
        searchConfig.preFilterBitset = bitsetBuf.data();
        searchConfig.preFilterBitsetSize = bitsetBuf.size();
      }
    }

    pangrova::vector::diskann::DiskAnnSearcher searcher(
        *index, [&](int64_t nodeId, float* buf, int64_t dim) {
          return index->readVector(nodeId, buf, dim);
        });
    auto results = searcher.search(query.data(), searchConfig);

    // Encode results: [count:4B] [rowId:8B, distance:8B]*N
    int count = static_cast<int>(results.size());
    int resultSize = 4 + count * 16;
    std::vector<uint8_t> resultBuf(resultSize);
    auto* p = resultBuf.data();
    std::memcpy(p, &count, 4); p += 4;
    for (const auto& r : results) {
      std::memcpy(p, &r.nodeId, 8); p += 8;
      double dist = static_cast<double>(r.distance);
      std::memcpy(p, &dist, 8); p += 8;
    }

    jbyteArray result = env->NewByteArray(resultSize);
    env->SetByteArrayRegion(result, 0, resultSize,
                            reinterpret_cast<const jbyte*>(resultBuf.data()));
    return static_cast<jbyteArray>(frame.popWithResult(result));
  } JNI_CATCH_AND_THROW(env)
  return nullptr;
}

/*
 * Class:     org_pangrova_worker_ai_vector_DiskAnnEngine
 * Method:    nativeWarmUp
 * Signature: (Ljava/lang/String;)V
 *
 * Triggers DiskAnnIndexCache to load the index from disk into memory without
 * performing a search. Eliminates cold-start latency for the first real query.
 */
JNIEXPORT void JNICALL
Java_org_pangrova_worker_ai_vector_DiskAnnEngine_nativeWarmUp(
    JNIEnv* env, jclass, jstring indexPath) {
  try {
    pangrova::jni::JniLocalFrameGuard frame(env, 16);
    if (!frame.valid()) {
      pangrova::jni::throwNativeBridgeException(env, "PushLocalFrame failed in nativeWarmUp");
      return;
    }

    std::string path = jstringToStd(env, indexPath);

    // Use dimension 0 as a hint — the cache will read actual dimension from
    // the index metadata file. This just pre-loads the graph and PQ codes.
    auto index = pangrova::vector::diskann::DiskAnnIndexCache::instance()
        .getOrLoad(path, 0);
    // Index is now in LRU cache — subsequent searches will hit cache.
    (void)index;
  } JNI_CATCH_AND_THROW(env)
}

}  // extern "C"
