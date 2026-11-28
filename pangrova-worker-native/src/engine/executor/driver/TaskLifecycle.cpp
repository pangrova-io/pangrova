// TaskLifecycle: Thread-safe state machine for task lifecycle.
// Extracted from TaskExecutor to enforce single-responsibility.
#include "engine/executor/driver/TaskLifecycle.h"

#include "jni/NativeStackTrace.h"

#include <stdexcept>

namespace pangrova::executor {
namespace {
protocol::v2::ExecutionError nativeFailure(const std::string& message) {
  protocol::v2::ExecutionError error;
  error.set_error_class(protocol::v2::ERROR_CLASS_INTERNAL);
  error.set_code("NATIVE_TASK_FAILED");
  error.set_message(message);
  error.set_retryable(false);
  error.set_failed_component("native-executor");
  return error;
}
}  // namespace

bool TaskLifecycle::transitionTo(ExecutorState new_state) {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return transitionToLocked(new_state);
}

bool TaskLifecycle::transitionToLocked(ExecutorState new_state) {
  auto current = state_.load();
  while (true) {
    if (isTerminal(current)) return false;
    if (!isValidTransition(current, new_state)) return false;
    if (state_.compare_exchange_weak(current, new_state)) {
      if (isTerminal(new_state)) terminal_signal_.notify_all();
      return true;
    }
  }
}

ExecutorState TaskLifecycle::state() const { return state_.load(); }

ExecutorState TaskLifecycle::awaitTerminal(std::chrono::milliseconds timeout) const {
  std::unique_lock<std::mutex> lock(error_mutex_);
  terminal_signal_.wait_for(lock, timeout, [this] { return isTerminal(); });
  return state_.load();
}

bool TaskLifecycle::isTerminal() const { return isTerminal(state_.load()); }

void TaskLifecycle::recordExecutionError(
    const protocol::v2::ExecutionError& error) {
  if (!protocol::v2::ErrorClass_IsValid(error.error_class()) ||
      error.error_class() == protocol::v2::ERROR_CLASS_UNSPECIFIED ||
      error.code().empty() || error.message().empty() ||
      error.failed_component().empty() ||
      !error.GetReflection()->GetUnknownFields(error).empty()) {
    throw std::invalid_argument("Canonical execution failure is incomplete");
  }
  // Allocate before taking ownership. A failed copy cannot replace the first
  // failure or publish a partially populated diagnostic.
  auto accepted = std::make_optional(error);
  std::lock_guard<std::mutex> lock(error_mutex_);
  if (!error_.has_value() && state_.load() != ExecutorState::FINISHED) {
    error_ = std::move(accepted);
  }
}

TaskLifecycle::Snapshot TaskLifecycle::snapshot() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return {state_.load(), error_, native_stack_trace_, result_delivery_};
}

void TaskLifecycle::appendError(const std::string& message) {
  if (message.empty()) return;
  std::lock_guard<std::mutex> lock(error_mutex_);
  if (!error_.has_value()) error_ = nativeFailure(message);
  else error_->set_message(error_->message() + "; " + message);
}

std::string TaskLifecycle::errorMessage() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return error_.has_value() ? error_->message() : std::string{};
}

std::string TaskLifecycle::nativeStackTrace() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return native_stack_trace_;
}

void TaskLifecycle::notifyTaskComplete() {
  transitionTo(ExecutorState::FLUSHING);
}

bool TaskLifecycle::finishResultStream(protocol::v2::NativeResultDeliveryStats delivery) {
  std::lock_guard<std::mutex> lock(error_mutex_);
  if (error_ || !transitionToLocked(ExecutorState::FLUSHING)) return false;
  result_delivery_.emplace(std::move(delivery));
  return transitionToLocked(ExecutorState::FINISHED);
}

void TaskLifecycle::notifyTaskFailed(const std::string& message) {
  // Capture stack trace and set error BEFORE transitioning state.
  // This avoids a race where observers see FAILED state but errorMessage()
  // is still empty (the transition publishes the terminal state atomically,
  // but setError was previously called after the publish).
  auto stack_trace = pangrova::jni::captureNativeStackTrace();
  auto generic_error = nativeFailure(message);
  // Hold the error lock across the terminal-state publication. A reader that
  // observes FAILED and then asks for the error blocks until both fields are
  // populated, eliminating the previous FAILED-with-empty-error race.
  std::lock_guard<std::mutex> lock(error_mutex_);
  if (isTerminal(state_.load())) return;
  if (!error_.has_value()) error_ = std::move(generic_error);
  native_stack_trace_ = std::move(stack_trace);
  transitionToLocked(ExecutorState::FAILED);
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
