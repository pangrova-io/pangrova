package org.pangrova.testing.integration.e2e;

import java.util.Objects;

/**
 * Validates that observability signals are captured during chaos scenarios.
 *
 * <p>Checks that traces, metrics, and audit events are all present
 * in the given {@link ObservabilitySnapshot}.
 */
public final class ChaosObservabilityValidator {

    /** Snapshot of observability signal capture state. */
    public record ObservabilitySnapshot(
        boolean tracesCaptured,
        boolean metricsCaptured,
        boolean auditCaptured) {}

    private ChaosObservabilityValidator() {}

    /**
     * Validate that all observability signals were captured.
     *
     * @param snapshot the observability snapshot to validate; must not be null
     * @return true if all signals (traces, metrics, audit) were captured
     */
    public static boolean validate(ObservabilitySnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        return snapshot.tracesCaptured()
            && snapshot.metricsCaptured()
            && snapshot.auditCaptured();
    }
}
