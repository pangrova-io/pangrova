// Thread-safe state machine for task lifecycle.
// Transitions: PENDING → RUNNING → FLUSHING → FINISHED/FAILED/CANCELLED
// Terminal states are monotonic — no further transitions once reached.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <string>

#include "pangrova/protocol/v2/result.pb.h"

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
  struct Snapshot {
    ExecutorState state;
    std::optional<protocol::v2::ExecutionError> error;
    std::string native_stack_trace;
  };
  TaskLifecycle() = default;

  // Non-copyable, non-movable (contains atomic + mutex).
  TaskLifecycle(const TaskLifecycle&) = delete;
  TaskLifecycle& operator=(const TaskLifecycle&) = delete;

  // Attempt a state transition. Returns true if successful.
  bool transitionTo(ExecutorState new_state);

  // Current state (atomic load).
  ExecutorState state() const;

  // Returns on terminal publication or timeout with the state then observed.
  ExecutorState awaitTerminal(std::chrono::milliseconds timeout) const;

  // Whether the current state is terminal (FINISHED/FAILED/CANCELLED).
  bool isTerminal() const;

  // Records an accepted, canonical failure before its producer wakes consumers.
  // This is memory-only: no cancellation or ingress calls while holding the lock.
  void recordExecutionError(const protocol::v2::ExecutionError& error);
  Snapshot snapshot() const;

  // Retain cleanup diagnostics without rewriting an already published terminal state.
  void appendError(const std::string& message);

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
  bool transitionToLocked(ExecutorState new_state);
  bool isTerminal(ExecutorState s) const;
  bool isValidTransition(ExecutorState from, ExecutorState to) const;

  std::atomic<ExecutorState> state_{ExecutorState::PENDING};
  mutable std::mutex error_mutex_;
  mutable std::condition_variable terminal_signal_;
  std::optional<protocol::v2::ExecutionError> error_;
  std::string native_stack_trace_;
};

}  // namespace pangrova::executor
