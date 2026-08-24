// Deserializes TaskRequest protobuf bytes into TaskExecutorConfig.
#pragma once

#include <vector>

#include "executor/driver/TaskExecutor.h"

namespace pangrova::jni {

// Parse TaskRequest protobuf bytes into TaskExecutorConfig.
// Falls back to legacy length-prefixed format when proto is unavailable.
executor::TaskExecutorConfig parseTaskConfig(
    const std::vector<uint8_t>& data);

// Parse CreateExecutorCommand protobuf bytes into TaskExecutorConfig.
// Unwraps the command envelope to extract the inner TaskRequest bytes
// and the is_result_task flag. When is_result_task is true,
// VeloxPlanConverter wraps the plan root with a PartitionedOutputNode.
executor::TaskExecutorConfig parseCreateExecutorCommand(
    const std::vector<uint8_t>& data);

}  // namespace pangrova::jni
