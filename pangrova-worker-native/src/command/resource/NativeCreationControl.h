#pragma once

#include <cstdint>
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni {
protocol::v2::RuntimeCommandResponse recoverExecutorCreation(
    const protocol::v2::RuntimeCommand& command, int64_t executor_handle);
}
