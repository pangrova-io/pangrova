#pragma once

#include "engine/velox/runner/VeloxTaskRunner.h"

namespace pangrova::velox { class VeloxPlanConverter; }
namespace pangrova::executor {
struct TaskExecutorConfig;
struct WorkerLocalSpillPolicy;

velox::VeloxTaskRunnerConfig makeRunnerConfig(const TaskExecutorConfig& config,
    const velox::VeloxPlanConverter& converter, const WorkerLocalSpillPolicy& spillPolicy);
}  // namespace pangrova::executor
