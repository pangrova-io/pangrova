package org.pangrova.databuild.api.build;

import org.pangrova.databuild.api.execution.DataBuildStageGraph;

import org.pangrova.security.api.authz.DeferredAuthorizationScope;

/**
 * Immutable control-plane values for dispatching Native artifact work.
 * Encoding admission belongs to the submission factory and durable store boundary.
 */
public record DataBuildDispatchSpec(
        String jobId,
        DataBuildStageGraph stageGraph,
        String tenantId,
        String principal,
        DeferredAuthorizationScope authorizationScope,
        long maxMemoryBytes,
        int maxThreads,
        long maxSpillBytes,
        long taskTimeoutMillis) {

    public DataBuildDispatchSpec {
        requireText(jobId, "jobId");
        java.util.Objects.requireNonNull(stageGraph, "stageGraph");
        requireText(tenantId, "tenantId");
        requireText(principal, "principal");
        java.util.Objects.requireNonNull(authorizationScope, "authorizationScope");
        requireResources(maxMemoryBytes, maxThreads, maxSpillBytes, taskTimeoutMillis);
    }

    private static void requireResources(
            long maxMemoryBytes, int maxThreads,
            long maxSpillBytes, long taskTimeoutMillis) {
        if (maxMemoryBytes <= 0 || maxThreads <= 0 || maxSpillBytes <= 0
                || taskTimeoutMillis <= 0) {
            throw new IllegalArgumentException("artifact resource grants and timeout must be positive");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    /** Positive native resource limits associated with an artifact dispatch. */
    public record ResourceGrant(
            long maxMemoryBytes,
            int maxThreads,
            long maxSpillBytes,
            long taskTimeoutMillis) {

        /** Validates all limits when the resource grant is created. */
        public ResourceGrant {
            requireResources(maxMemoryBytes, maxThreads, maxSpillBytes, taskTimeoutMillis);
        }
    }
}
