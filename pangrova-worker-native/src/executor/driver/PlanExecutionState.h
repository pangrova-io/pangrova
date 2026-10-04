#pragma once

#include <exception>
#include <memory>
#include <thread>
#include <vector>

#include "artifact/ArtifactRetrievalPlanner.h"
#include "executor/driver/PlanExecutionDriver.h"
#include "velox/plan/ArtifactBuildOperator.h"
#include "velox/runner/SingleMatchJoinRequirement.h"
#include "velox/runner/VeloxTaskRunner.h"

namespace pangrova::executor {

// Task-local owners cross the continuation boundary together. This is execution
// lifetime state, not a second plan or protocol representation.
struct PlanExecutionDriver::Execution {
  WorkerLocalSpillPolicy spillPolicy;
  std::shared_ptr<velox::ArtifactBuildTaskContext> artifactContext;
  std::shared_ptr<artifact::ArtifactRetrievalPreparation> retrieval;
  std::shared_ptr<velox::VeloxTaskRunner> runner;
  facebook::velox::RowTypePtr writeOutputType;
  std::vector<velox::SingleMatchJoinRequirement> singleMatchRequirements;
  std::unique_ptr<std::thread> shuffleDrain;
  std::unique_ptr<std::thread> writeDrain;
  std::exception_ptr shuffleFailure;
  std::exception_ptr writeFailure;
};

}  // namespace pangrova::executor
