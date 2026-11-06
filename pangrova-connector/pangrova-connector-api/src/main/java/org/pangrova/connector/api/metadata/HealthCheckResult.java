package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Result of a connector health check, including status, latency, and optional error message.
 */
@Experimental
public record HealthCheckResult(HealthStatus status, long latencyMs, String errorMessage) {

    public enum HealthStatus {
        HEALTHY,
        DEGRADED,
        UNHEALTHY
    }

    public static HealthCheckResult healthy() {
        return new HealthCheckResult(HealthStatus.HEALTHY, 0, "");
    }

    public static HealthCheckResult degraded(long latencyMs, String msg) {
        return new HealthCheckResult(HealthStatus.DEGRADED, latencyMs, msg);
    }

    public static HealthCheckResult unhealthy(String msg) {
        return new HealthCheckResult(HealthStatus.UNHEALTHY, 0, msg);
    }
}
