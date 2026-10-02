#pragma once

#include "executor/driver/TaskExecutor.h"
#include "pangrova/protocol/v2/task.pb.h"

namespace pangrova::jni {

// One canonical physical output admission path; no connector SDK, JNI row sink or new protocol.
executor::WriteConfigInfo parsePhysicalWriteOptions(
    const protocol::v2::WriteTaskOptions& options,
    const protocol::v2::TaskAttemptId& attempt);

}  // namespace pangrova::jni
