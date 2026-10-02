// Deserializes TaskRequest protobuf bytes into TaskExecutorConfig.
#pragma once

#include <vector>

#include "executor/driver/TaskExecutor.h"

namespace pangrova::jni {

// Parse canonical v2 TaskRequest protobuf bytes into TaskExecutorConfig.
// Invalid, empty, mismatched or legacy payloads fail closed.
executor::TaskExecutorConfig parseTaskConfig(
    const std::vector<uint8_t>& data);

}  // namespace pangrova::jni
