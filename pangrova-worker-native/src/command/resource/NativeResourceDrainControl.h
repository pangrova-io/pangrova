#pragma once

#include <cstdint>
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni {
protocol::v2::RuntimeCommandResponse resourceDrainEvidence(
    const protocol::v2::RuntimeCommand& command, int64_t executorHandle);
}  // namespace pangrova::jni
