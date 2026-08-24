package org.pangrova.coordinator.query.execution.dispatch.dag.insert;

/**
 * Immutable configuration for INSERT SELECT pipeline execution.
 */
public record InsertSelectConfig(long taskRunningTimeoutMs) {

    private static final long DEFAULT_TIMEOUT_MS = 60_000;

    public InsertSelectConfig {
        if (taskRunningTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "taskRunningTimeoutMs must be positive, got: " + taskRunningTimeoutMs);
        }
    }

    public static InsertSelectConfig defaults() {
        return new InsertSelectConfig(DEFAULT_TIMEOUT_MS);
    }
}
