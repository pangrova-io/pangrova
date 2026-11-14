// Deserializes TaskRequest protobuf bytes into TaskExecutorConfig.
#pragma once

#include <vector>

#include "engine/executor/driver/TaskExecutor.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni {

// Parse canonical v2 TaskRequest protobuf bytes into TaskExecutorConfig.
// Invalid, empty, mismatched or legacy payloads fail closed.
executor::TaskExecutorConfig parseTaskConfig(
    const std::vector<uint8_t>& data);

// Parse once before reserving creation ownership. Configuration decoding then
// consumes that same message; identity recovery does not decode a second plan.
protocol::v2::TaskRequest parseTaskRequest(const std::vector<uint8_t>& data);
executor::TaskExecutorConfig taskConfigFromRequest(const protocol::v2::TaskRequest& request);
protocol::v2::ExecutorCreationRecoveryRequest creationIdentity(protocol::v2::TaskRequest& request);
void validateCreationIdentity(const protocol::v2::ExecutorCreationRecoveryRequest& identity);

}  // namespace pangrova::jni
