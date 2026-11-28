#pragma once

#include <cstdint>
#include <string>

namespace pangrova::protocol::v2 {
class ExecutionError;
class NativeResultDeliveryStats;
}

namespace pangrova::executor {

// The handle owner resolves the exact live executor for each report. Reporters
// are borrowed by retained results and must outlive their execution process.
class TaskFailureReporter {
 public:
  virtual ~TaskFailureReporter() = default;
  virtual void reportExecutionError(int64_t handle, const protocol::v2::ExecutionError& error) const = 0;
  virtual void reportResultStreamFinished(
      int64_t handle, const protocol::v2::NativeResultDeliveryStats& delivery) const = 0;
  virtual void reportResultStreamFailure(int64_t handle, const std::string& message) const = 0;
};

}  // namespace pangrova::executor
