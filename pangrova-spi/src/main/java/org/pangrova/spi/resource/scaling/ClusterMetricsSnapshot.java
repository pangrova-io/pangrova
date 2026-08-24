package org.pangrova.spi.resource.scaling;

import org.pangrova.spi.annotation.Experimental;

/**
 * A point-in-time snapshot of cluster-wide resource metrics fed to {@link AutoScaler}.
 *
 * <p>The metrics cover queue depth, per-tenant pressure, CPU/memory utilisation from
 * worker heartbeats, and the current worker count. All values must be non-negative.</p>
 *
 * @param currentWorkers    number of active workers in the cluster
 * @param queueDepth        total queries waiting for scheduling
 * @param avgCpuUtilisation average CPU utilisation across workers (0.0 to 1.0)
 * @param avgMemoryUtilisation average memory utilisation across workers (0.0 to 1.0)
 * @param maxTenantPressure highest per-tenant composite pressure score (0.0 to 1.0)
 */
@Experimental(reason = "G-RES-03: auto-scaling SPI")
public record ClusterMetricsSnapshot(
        int currentWorkers,
        int queueDepth,
        double avgCpuUtilisation,
        double avgMemoryUtilisation,
        double maxTenantPressure) {

    public ClusterMetricsSnapshot {
        if (currentWorkers < 0) {
            throw new IllegalArgumentException("currentWorkers must be non-negative");
        }
        if (queueDepth < 0) {
            throw new IllegalArgumentException("queueDepth must be non-negative");
        }
        validateRatio(avgCpuUtilisation, "avgCpuUtilisation");
        validateRatio(avgMemoryUtilisation, "avgMemoryUtilisation");
        validateRatio(maxTenantPressure, "maxTenantPressure");
    }

    private static void validateRatio(double value, String fieldName) {
        if (value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(
                    fieldName + " must be in [0.0, 1.0], got " + value);
        }
    }
}
