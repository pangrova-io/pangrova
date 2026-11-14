package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.enums.ConnectorHealthStatus;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Result of a connector health check, including status, latency, and optional error message.
 */
@Experimental
public record HealthCheckResult(ConnectorHealthStatus status, long latencyMs, String errorMessage) {


    public static HealthCheckResult healthy() {
        return new HealthCheckResult(ConnectorHealthStatus.HEALTHY, 0, "");
    }

    public static HealthCheckResult degraded(long latencyMs, String msg) {
        return new HealthCheckResult(ConnectorHealthStatus.DEGRADED, latencyMs, msg);
    }

    public static HealthCheckResult unhealthy(String msg) {
        return new HealthCheckResult(ConnectorHealthStatus.UNHEALTHY, 0, msg);
    }
}
