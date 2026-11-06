package org.pangrova.planner.core.throttle;

/**
 * Snapshot of cluster resource usage for adaptive throttling.
 * All values are ratios in [0.0, 1.0].
 *
 * @param cpuUsage    cluster CPU usage (0.0 = idle, 1.0 = fully loaded)
 * @param memoryUsage cluster memory usage (0.0 = free, 1.0 = fully used)
 * @param errorRate   query error rate (0.0 = no errors, 1.0 = all errors)
 */
public record ClusterMetrics(double cpuUsage, double memoryUsage, double errorRate) {

    public ClusterMetrics {
        validateRange("cpuUsage", cpuUsage);
        validateRange("memoryUsage", memoryUsage);
        validateRange("errorRate", errorRate);
    }

    private static void validateRange(String name, double value) {
        if (value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(
                    name + " must be in [0.0, 1.0], got: " + value);
        }
    }

    boolean isOverloaded() {
        return cpuUsage > 0.9 || memoryUsage > 0.9 || errorRate > 0.1;
    }

    boolean isUnderloaded() {
        return cpuUsage < 0.5 && memoryUsage < 0.5 && errorRate < 0.01;
    }
}
