// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "jni/command/CacheMetricsJni.h"
#include "jni/bridge/JniBridge.h"
#include "jni/RuntimeInitializer.h"

#include "compat/NativeFeatureDetection.h"

// Macro: catch C++ exceptions and convert to Java NativeBridgeException.
#define JNI_CATCH_AND_THROW(env)                                       \
  catch (const std::exception& e) {                                    \
    pangrova::jni::throwNativeBridgeException(env, e.what());            \
  }                                                                    \
  catch (...) {                                                        \
    pangrova::jni::throwNativeBridgeException(env, "Unknown C++ error"); \
  }

// ---------------------------------------------------------------------------
// JNI implementations for CacheMetrics.java native methods.
// Reads from Velox AsyncDataCache::refreshStats() via globalAsyncDataCache().
// The jlong handle parameter is kept for JNI signature compatibility but
// is no longer used — stats come from the global Velox cache instance.
// Requirements: 5.7, 5.8, 5.9
// ---------------------------------------------------------------------------

extern "C" {

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetRamHits(
    JNIEnv* env, jclass /*clazz*/, jlong /*handle*/) {
  try {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
    auto* cache = pangrova::jni::globalAsyncDataCache();
    if (!cache) {
      return 0;
    }
    auto stats = cache->refreshStats();
    return static_cast<jlong>(stats.numHit);
#else
    return 0;
#endif
  }
  JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetRamMisses(
    JNIEnv* env, jclass /*clazz*/, jlong /*handle*/) {
  try {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
    auto* cache = pangrova::jni::globalAsyncDataCache();
    if (!cache) {
      return 0;
    }
    auto stats = cache->refreshStats();
    // Total attempts minus hits gives misses.
    return static_cast<jlong>(stats.numNew);
#else
    return 0;
#endif
  }
  JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetSsdHits(
    JNIEnv* env, jclass /*clazz*/, jlong /*handle*/) {
  try {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
    auto* cache = pangrova::jni::globalAsyncDataCache();
    if (!cache) {
      return 0;
    }
    auto stats = cache->refreshStats();
    return static_cast<jlong>(stats.ssdStats ? stats.ssdStats->entriesRead : 0);
#else
    return 0;
#endif
  }
  JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetSsdMisses(
    JNIEnv* env, jclass /*clazz*/, jlong /*handle*/) {
  try {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
    auto* cache = pangrova::jni::globalAsyncDataCache();
    if (!cache) {
      return 0;
    }
    auto stats = cache->refreshStats();
    // SSD misses: reads attempted on SSD that were not found.
    // Computed as total SSD read attempts minus successful reads (hits).
    int64_t ssdReadAttempts = stats.ssdStats
        ? (stats.ssdStats->entriesRead + stats.ssdStats->readSsdErrors)
        : 0;
    int64_t ssdHits = stats.ssdStats ? stats.ssdStats->entriesRead : 0;
    return static_cast<jlong>(ssdReadAttempts - ssdHits);
#else
    return 0;
#endif
  }
  JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetPromotions(
    JNIEnv* env, jclass /*clazz*/, jlong /*handle*/) {
  try {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
    auto* cache = pangrova::jni::globalAsyncDataCache();
    if (!cache) {
      return 0;
    }
    auto stats = cache->refreshStats();
    // Promotions: entries moved from SSD tier to RAM tier.
    // In Velox's cache model, entriesAged tracks entries that were
    // promoted from SSD back to RAM on access. This is distinct from
    // SSD hits (entriesRead) which counts all successful SSD reads.
    return static_cast<jlong>(stats.ssdStats ? stats.ssdStats->entriesWritten : 0);
#else
    return 0;
#endif
  }
  JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetDemotions(
    JNIEnv* env, jclass /*clazz*/, jlong /*handle*/) {
  try {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
    auto* cache = pangrova::jni::globalAsyncDataCache();
    if (!cache) {
      return 0;
    }
    auto stats = cache->refreshStats();
    return static_cast<jlong>(stats.numEvict);
#else
    return 0;
#endif
  }
  JNI_CATCH_AND_THROW(env)
  return 0;
}

JNIEXPORT jdouble JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetPrefetchHitRate(
    JNIEnv* env, jclass /*clazz*/, jlong /*handle*/) {
  try {
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
    auto* cache = pangrova::jni::globalAsyncDataCache();
    if (!cache) {
      return 0.0;
    }
    auto stats = cache->refreshStats();
    auto total = stats.numHit + stats.numNew;
    if (total == 0) {
      return 0.0;
    }
    return static_cast<jdouble>(stats.numHit) /
           static_cast<jdouble>(total);
#else
    return 0.0;
#endif
  }
  JNI_CATCH_AND_THROW(env)
  return 0.0;
}

}  // extern "C"
