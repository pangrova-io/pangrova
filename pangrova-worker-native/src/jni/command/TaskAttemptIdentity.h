// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <stdexcept>

#include "executor/driver/TaskExecutor.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni {

// The admitted callback context already retains the complete canonical identity.
// Reconstructing a subset drops new ownership fields from reports and commands.
inline protocol::v2::TaskAttemptId admittedTaskAttempt(
    const executor::TaskExecutor& executor) {
  protocol::v2::ExecutionCallbackContext context;
  if (!context.ParseFromString(executor.callbackContext()) ||
      !context.has_attempt()) {
    throw std::runtime_error("Executor has no admitted canonical attempt context");
  }
  return context.attempt();
}

}  // namespace pangrova::jni
