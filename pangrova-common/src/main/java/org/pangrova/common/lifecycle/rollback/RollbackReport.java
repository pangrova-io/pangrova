package org.pangrova.common.lifecycle.rollback;

import org.pangrova.common.enums.BootstrapPhase;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable report for a single rollback attempt during bootstrap failure recovery.
 *
 * <p>Each report captures the phase being rolled back, the executor that performed
 * the rollback, whether it succeeded, and any error details if it failed.</p>
 *
 * @see RollbackAggregateReport
 */
public final class RollbackReport {

    private final BootstrapPhase phase;
    private final String executorName;
    private final boolean success;
    private final String errorMessage;
    private final Instant timestamp;

    private RollbackReport(BootstrapPhase phase,
                           String executorName,
                           boolean success,
                           String errorMessage,
                           Instant timestamp) {
        this.phase = Objects.requireNonNull(phase, "phase");
        this.executorName = Objects.requireNonNull(executorName, "executorName");
        this.success = success;
        this.errorMessage = errorMessage;
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
    }

    /**
     * Creates a report for a successful rollback.
     */
    public static RollbackReport success(BootstrapPhase phase, String executorName, Instant timestamp) {
        return new RollbackReport(phase, executorName, true, null, timestamp);
    }

    /**
     * Creates a report for a failed rollback.
     */
    public static RollbackReport failure(BootstrapPhase phase,
                                         String executorName,
                                         String errorMessage,
                                         Instant timestamp) {
        Objects.requireNonNull(errorMessage, "errorMessage");
        return new RollbackReport(phase, executorName, false, errorMessage, timestamp);
    }

    public BootstrapPhase phase() {
        return phase;
    }

    public String executorName() {
        return executorName;
    }

    public boolean success() {
        return success;
    }

    /**
     * Error message from the rollback failure, or {@code null} if rollback succeeded.
     */
    public String errorMessage() {
        return errorMessage;
    }

    public Instant timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "RollbackReport{phase=" + phase
                + ", executor=" + executorName
                + ", success=" + success
                + (errorMessage != null ? ", error=" + errorMessage : "")
                + ", timestamp=" + timestamp + "}";
    }
}
