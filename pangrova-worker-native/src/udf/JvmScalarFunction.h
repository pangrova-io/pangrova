// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <jni.h>

#include <string>
#include <vector>

#include "udf/NativeUdfRegistry.h"

namespace pangrova::udf {

/// Installs the Java callback target used by JVM-backed vector functions.
/// Replaces any previously installed target and caches all JNI classes and
/// method IDs needed by native execution threads.
void installJvmFunctionCallbacks(JNIEnv* env, jobject callback);

/// Releases the callback target and cached JNI global references.
void clearJvmFunctionCallbacks(JNIEnv* env);

/// Builds a scalar UDF specification backed by a batched Java callback.
ScalarUdfSpec makeJvmScalarFunctionSpec(
    const std::string& name,
    const std::vector<std::string>& argumentTypes,
    const std::string& returnType);

}  // namespace pangrova::udf
