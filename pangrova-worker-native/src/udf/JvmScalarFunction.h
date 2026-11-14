// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <jni.h>

#include <memory>
#include <string>
#include <vector>

#include "udf/NativeUdfRegistry.h"

namespace pangrova::executor {
class TaskFailureReporter;
}

namespace pangrova::protocol::v2 {
class NativeFunctionDescriptor;
}

namespace pangrova::udf {

class JvmFunctionCallbacks;
using JvmFunctionCallbackOwner = std::shared_ptr<const JvmFunctionCallbacks>;

/// Prepares an immutable callback generation without changing the active one.
/// JNI references and method IDs are validated before registration can publish.
JvmFunctionCallbackOwner prepareJvmFunctionCallbacks(
    JNIEnv* env, jobject callback, const executor::TaskFailureReporter& failures);

/// Publishes a successfully registered generation. Existing compiled factories
/// retain their own owner; publication never redirects an already bound call.
void publishJvmFunctionCallbacks(JvmFunctionCallbackOwner owner);

/// Binds a parsed specification to exactly one prepared callback generation.
void bindJvmFunctionCallbacks(ScalarUdfSpec& spec, JvmFunctionCallbackOwner owner);

/// Releases the callback target and cached JNI global references.
void clearJvmFunctionCallbacks(JNIEnv* env);

/// Validates one already-parsed canonical descriptor before owner retention.
/// Wire absence is rejected; the pure factory's strict default is not a protocol fallback.
ScalarUdfSpec makeJvmScalarFunctionSpec(
    const protocol::v2::NativeFunctionDescriptor& descriptor);

/// Builds a scalar UDF specification backed by a batched Java callback.
/// Determinism is supplied by the canonical callback declaration.
ScalarUdfSpec makeJvmScalarFunctionSpec(
    const std::string& name,
    const std::vector<std::string>& argumentTypes,
    const std::string& returnType,
    bool deterministic,
    bool defaultNullBehavior = facebook::velox::exec::VectorFunctionMetadata{}.defaultNullBehavior);

}  // namespace pangrova::udf
