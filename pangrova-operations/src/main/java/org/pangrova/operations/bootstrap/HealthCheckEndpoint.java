package org.pangrova.operations.bootstrap;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Health check endpoint with node-type-specific readiness logic.
 * Coordinator: HTTP + ≥1 Worker + ≥1 Catalog.
 * Worker: HTTP + Transport + Native.
 */
public final class HealthCheckEndpoint {

    private final NodeType nodeType;
    private final HealthConditions conditions;

    public HealthCheckEndpoint(NodeType nodeType, HealthConditions conditions) {
        this.nodeType = Objects.requireNonNull(nodeType, "nodeType");
        this.conditions = Objects.requireNonNull(conditions, "conditions");
    }

    /** Returns true if the node passes all readiness checks for its type. */
    public boolean isReady() {
        return switch (nodeType) {
            case COORDINATOR -> isCoordinatorReady();
            case WORKER -> isWorkerReady();
        };
    }

    /** Builds the full health status including per-component check results. */
    public HealthStatus health() {
        Map<String, Boolean> checks = buildChecks();
        long lastProcessedId = conditions.lastProcessedInvalidationId();
        int rsPort = conditions.resultServerPort();
        boolean rsRunning = conditions.resultServerRunning();
        return new HealthStatus(isReady(), nodeType, checks, lastProcessedId, rsPort, rsRunning);
    }

    private boolean isCoordinatorReady() {
        return conditions.isHttpReady()
                && conditions.activeWorkerCount() >= 1
                && conditions.activeCatalogCount() >= 1;
    }

    private boolean isWorkerReady() {
        return conditions.isHttpReady()
                && conditions.isTransportReady()
                && conditions.isNativeReady();
    }

    private Map<String, Boolean> buildChecks() {
        Map<String, Boolean> checks = new LinkedHashMap<>();
        checks.put("http", conditions.isHttpReady());
        checks.put("transport", conditions.isTransportReady());
        checks.put("native", conditions.isNativeReady());
        checks.put("resultServerRunning", conditions.resultServerRunning());
        checks.put("hasWorkers", conditions.activeWorkerCount() >= 1);
        checks.put("hasCatalogs", conditions.activeCatalogCount() >= 1);
        return checks;
    }

    public enum NodeType {
        COORDINATOR,
        WORKER
    }

    /**
     * SPI for querying node health conditions.
     */
    public interface HealthConditions {

        boolean isHttpReady();

        boolean isTransportReady();

        boolean isNativeReady();

        int activeWorkerCount();

        int activeCatalogCount();

        /**
         * Returns the bound port of the result server, or -1 if not running.
         */
        default int resultServerPort() {
            return -1;
        }

        /**
         * Returns whether the result server is running and accepting requests.
         */
        default boolean resultServerRunning() {
            return false;
        }

        /**
         * Returns the last processed invalidation log ID for DDL propagation tracking.
         * Returns -1 if invalidation polling is not configured.
         */
        default long lastProcessedInvalidationId() {
            return -1;
        }
    }
}
