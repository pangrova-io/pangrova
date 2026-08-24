package org.pangrova.operations.bootstrap;

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

    /**
     * Backward-compatible constructor without result server fields.
     */
    public HealthStatus(boolean ready, HealthCheckEndpoint.NodeType nodeType,
                        Map<String, Boolean> checks, long lastProcessedInvalidationId) {
        this(ready, nodeType, checks, lastProcessedInvalidationId, -1, false);
    }

    /**
     * Backward-compatible constructor without lastProcessedInvalidationId.
     */
    public HealthStatus(boolean ready, HealthCheckEndpoint.NodeType nodeType,
                        Map<String, Boolean> checks) {
        this(ready, nodeType, checks, -1, -1, false);
    }
}
