#pragma once

#include "pangrova/protocol/v2/task.pb.h"
#include "engine/executor/driver/TaskExecutor.h"

namespace pangrova::jni {
void validateWorkerCopyInput(const protocol::v2::TaskRequest& request);
void applyWorkerCopyInput(const protocol::v2::TaskRequest& request, executor::TaskExecutorConfig& config);
}  // namespace pangrova::jni
