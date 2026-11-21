package org.pangrova.scheduler.api.placement;

import java.util.Objects;

/**
 * Immutable ranking input, captured after Core has enforced placement constraints.
 * Ratios are finite values in [0, 1]. This is advisory telemetry, not a reservation.
 */
public record WorkerSelectionCandidate(String workerId, double memoryPressureRatio,
                                       double cpuUtilization) {
    public WorkerSelectionCandidate {
        Objects.requireNonNull(workerId, "workerId");
        if (workerId.isBlank()) throw new IllegalArgumentException("workerId must not be blank");
        requireRatio(memoryPressureRatio, "memoryPressureRatio");
        requireRatio(cpuUtilization, "cpuUtilization");
    }

    private static void requireRatio(double value, String name) {
        if (!Double.isFinite(value) || value < 0 || value > 1) {
            throw new IllegalArgumentException(name + " must be finite and in [0, 1]");
        }
    }
}
