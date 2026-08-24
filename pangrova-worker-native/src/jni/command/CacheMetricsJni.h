// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once
#include <jni.h>

// JNI function declarations matching CacheMetrics.java native methods.
// Package: org.pangrova.worker.cache.metrics
//
// These are stubs — actual CacheManager wiring happens in Task 19.1.
// Requirements: 18.1, 18.2, 18.3

extern "C" {

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetRamHits(
    JNIEnv* env, jclass clazz, jlong handle);

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetRamMisses(
    JNIEnv* env, jclass clazz, jlong handle);

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetSsdHits(
    JNIEnv* env, jclass clazz, jlong handle);

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetSsdMisses(
    JNIEnv* env, jclass clazz, jlong handle);

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetPromotions(
    JNIEnv* env, jclass clazz, jlong handle);

JNIEXPORT jlong JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetDemotions(
    JNIEnv* env, jclass clazz, jlong handle);

JNIEXPORT jdouble JNICALL
Java_org_pangrova_worker_cache_metrics_CacheMetrics_nativeGetPrefetchHitRate(
    JNIEnv* env, jclass clazz, jlong handle);

}  // extern "C"
