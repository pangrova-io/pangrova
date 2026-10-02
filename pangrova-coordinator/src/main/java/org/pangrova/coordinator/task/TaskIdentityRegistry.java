package org.pangrova.coordinator.task;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;

import java.util.concurrent.ConcurrentHashMap;

/** Atomically retains the sole issued attempt and its exact request for fencing and speculative cloning. */
final class TaskIdentityRegistry {
    private final ConcurrentHashMap<String, Identity> identities = new ConcurrentHashMap<>();

    void registerRequest(String taskId, SubmitTaskRequest request) {
        requireTaskId(taskId);
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        var canonicalAttempt = request.taskRequest().getAttempt();
        if (!taskId.equals(canonicalAttempt.getTaskId())) {
            throw new IllegalArgumentException("taskId does not match request.taskId: taskId=" + taskId
                    + ", requestTaskId=" + canonicalAttempt.getTaskId());
        }
        if (!request.taskRequest().hasAttempt()) {
            throw new IllegalArgumentException("request.attempt must not be null");
        }
        TaskAttemptDescriptor issued = TaskAttemptDescriptor.from(canonicalAttempt);
        identities.compute(taskId, (ignored, existing) -> {
            requireSameAttempt(taskId, existing, issued);
            if (existing != null && existing.request() != null && !existing.request().equals(request)) {
                throw new IllegalStateException("Conflicting task request already registered: taskId=" + taskId);
            }
            return new Identity(issued, request);
        });
    }

    boolean removeRequest(String taskId, SubmitTaskRequest request) {
        var removed = new boolean[1];
        identities.computeIfPresent(taskId, (ignored, existing) -> {
            if (request.equals(existing.request())) {
                removed[0] = true;
                return null;
            }
            return existing;
        });
        return removed[0];
    }

    void registerAttempt(String taskId, TaskAttemptDescriptor attempt) {
        requireTaskId(taskId);
        if (attempt == null) {
            throw new IllegalArgumentException("attempt must not be null");
        }
        identities.compute(taskId, (ignored, existing) -> {
            requireSameAttempt(taskId, existing, attempt);
            return existing != null ? existing : new Identity(attempt, null);
        });
    }

    TaskAttemptDescriptor attempt(String taskId) {
        Identity identity = identities.get(taskId);
        return identity != null ? identity.attempt() : null;
    }

    SubmitTaskRequest request(String taskId) {
        Identity identity = identities.get(taskId);
        return identity != null ? identity.request() : null;
    }

    void remove(String taskId) {
        identities.remove(taskId);
    }

    private static void requireTaskId(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be null or blank");
        }
    }

    private static void requireSameAttempt(String taskId, Identity existing, TaskAttemptDescriptor requested) {
        if (existing != null && !existing.attempt().equals(requested)) {
            throw new IllegalStateException("Conflicting task attempt already registered: taskId=" + taskId
                    + ", existing=" + existing.attempt() + ", requested=" + requested);
        }
    }

    private record Identity(TaskAttemptDescriptor attempt, SubmitTaskRequest request) {}
}
