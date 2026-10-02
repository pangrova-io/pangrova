// Thread-safe state machine for task lifecycle.
// Transitions: PENDING → RUNNING → FLUSHING → FINISHED/FAILED/CANCELLED
// Terminal states are monotonic — no further transitions once reached.
#pragma once

#include <atomic>
#include <mutex>
#include <string>

namespace pangrova::executor {

enum class ExecutorState {
  PENDING,
  RUNNING,
  FLUSHING,
  FINISHED,
  FAILED,
  CANCELLED
};

// Thread-safe state machine for task lifecycle.
// Manages state transitions and error message propagation.
// @ThreadSafe — all public methods are safe for concurrent access.
class TaskLifecycle {
 public:
  TaskLifecycle() = default;

  // Non-copyable, non-movable (contains atomic + mutex).
  TaskLifecycle(const TaskLifecycle&) = delete;
  TaskLifecycle& operator=(const TaskLifecycle&) = delete;

  // Attempt a state transition. Returns true if successful.
  bool transitionTo(ExecutorState new_state);

  // Current state (atomic load).
  ExecutorState state() const;

  // Whether the current state is terminal (FINISHED/FAILED/CANCELLED).
  bool isTerminal() const;

  // Set error message (thread-safe via mutex).
  void setError(const std::string& message);

  // Retain cleanup diagnostics without rewriting an already published terminal state.
  void appendError(const std::string& message);

  // Set error message with native stack trace (thread-safe via mutex).
  void setError(const std::string& message, const std::string& stack_trace);

  // Retrieve error message (thread-safe via mutex).
  std::string errorMessage() const;

  // Retrieve native stack trace captured at failure point (thread-safe via mutex).
  std::string nativeStackTrace() const;

  // Convenience: transition to FLUSHING.
  void notifyTaskComplete();

  // Convenience: set error + transition to FAILED.
  // Captures C++ stack trace at the call site for propagation to Java.
  void notifyTaskFailed(const std::string& message);

 private:
  bool isTerminal(ExecutorState s) const;
  bool isValidTransition(ExecutorState from, ExecutorState to) const;

  std::atomic<ExecutorState> state_{ExecutorState::PENDING};
  mutable std::mutex error_mutex_;
  std::string error_message_;
  std::string native_stack_trace_;
};

}  // namespace pangrova::executor
