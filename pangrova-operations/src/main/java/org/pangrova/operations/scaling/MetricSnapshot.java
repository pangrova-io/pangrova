package org.pangrova.operations.scaling;

/**
 * Point-in-time snapshot of normalized cluster metrics (all values 0.0-1.0).
 */
public record MetricSnapshot(
        double cpuUtilization,
        double memoryUtilization,
        double queueDepthNormalized,
        double latencyNormalized) {

    public MetricSnapshot {
        validateRange("cpuUtilization", cpuUtilization);
        validateRange("memoryUtilization", memoryUtilization);
        validateRange("queueDepthNormalized", queueDepthNormalized);
        validateRange("latencyNormalized", latencyNormalized);
    }

    private static void validateRange(String name, double value) {
        if (value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(name + " must be in [0.0, 1.0], got: " + value);
        }
    }
}
