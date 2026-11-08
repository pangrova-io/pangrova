package org.pangrova.common.lifecycle;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable health check result for a node.
 */
public record HealthStatus(boolean ready, HealthCheckEndpoint.NodeType nodeType,
                           Map<String, Boolean> checks, long lastProcessedInvalidationId,
                           int resultServerPort, boolean resultServerRunning) {

    public HealthStatus {
        Objects.requireNonNull(nodeType, "nodeType");
        Objects.requireNonNull(checks, "checks");
        checks = Map.copyOf(checks);
    }

}
