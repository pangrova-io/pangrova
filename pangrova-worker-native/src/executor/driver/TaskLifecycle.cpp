// TaskLifecycle: Thread-safe state machine for task lifecycle.
// Extracted from TaskExecutor to enforce single-responsibility.
#include "executor/driver/TaskLifecycle.h"

#include "jni/bridge/NativeStackTrace.h"

namespace pangrova::executor {

bool TaskLifecycle::transitionTo(ExecutorState new_state) {
  auto current = state_.load();
  while (true) {
    if (isTerminal(current)) return false;
    if (!isValidTransition(current, new_state)) return false;
    if (state_.compare_exchange_weak(current, new_state)) return true;
  }
}

ExecutorState TaskLifecycle::state() const { return state_.load(); }

bool TaskLifecycle::isTerminal() const { return isTerminal(state_.load()); }

void TaskLifecycle::setError(const std::string& message) {
  std::lock_guard<std::mutex> lock(error_mutex_);
  error_message_ = message;
}

void TaskLifecycle::setError(const std::string& message,
                             const std::string& stack_trace) {
  std::lock_guard<std::mutex> lock(error_mutex_);
  error_message_ = message;
  native_stack_trace_ = stack_trace;
}

std::string TaskLifecycle::errorMessage() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return error_message_;
}

std::string TaskLifecycle::nativeStackTrace() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return native_stack_trace_;
}

void TaskLifecycle::notifyTaskComplete() {
  transitionTo(ExecutorState::FLUSHING);
}

void TaskLifecycle::notifyTaskFailed(const std::string& message) {
  // Capture stack trace and set error BEFORE transitioning state.
  // This avoids a race where observers see FAILED state but errorMessage()
  // is still empty (the transition publishes the terminal state atomically,
  // but setError was previously called after the publish).
  auto stack_trace = pangrova::jni::captureNativeStackTrace();
  // Only set error message if the transition to FAILED succeeds.
  // This ensures the first error wins (monotonic terminal state).
  if (transitionTo(ExecutorState::FAILED)) {
    setError(message, stack_trace);
  }
}

bool TaskLifecycle::isTerminal(ExecutorState s) const {
  return s == ExecutorState::FINISHED ||
         s == ExecutorState::FAILED ||
         s == ExecutorState::CANCELLED;
}

bool TaskLifecycle::isValidTransition(
    ExecutorState from, ExecutorState to) const {
  switch (from) {
    case ExecutorState::PENDING:
      return to == ExecutorState::RUNNING ||
             to == ExecutorState::FAILED ||
             to == ExecutorState::CANCELLED;
    case ExecutorState::RUNNING:
      return to == ExecutorState::FLUSHING ||
             to == ExecutorState::FAILED ||
             to == ExecutorState::CANCELLED;
    case ExecutorState::FLUSHING:
      return to == ExecutorState::FINISHED ||
             to == ExecutorState::FAILED ||
             to == ExecutorState::CANCELLED;
    default:
      return false;
  }
}

}  // namespace pangrova::executor
