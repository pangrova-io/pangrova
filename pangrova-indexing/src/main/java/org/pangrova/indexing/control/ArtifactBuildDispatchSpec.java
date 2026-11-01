package org.pangrova.indexing.control;

import org.pangrova.security.authz.DeferredAuthorizationScope;

/** Immutable, replayable control-plane payload for dispatching Native artifact work. */
public record ArtifactBuildDispatchSpec(
        String jobId,
        ArtifactStageGraph stageGraph,
        String tenantId,
        String principal,
        DeferredAuthorizationScope authorizationScope,
        long maxMemoryBytes,
        int maxThreads,
        long maxSpillBytes,
        long taskTimeoutMillis) {

    public ArtifactBuildDispatchSpec {
        requireText(jobId, "jobId");
        java.util.Objects.requireNonNull(stageGraph, "stageGraph");
        if (stageGraph.canonicalBytes().length > 16 * 1024 * 1024) {
            throw new IllegalArgumentException("durable graph exceeds metadata size limit");
        }
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

    /** Creates a dispatch specification from canonical binary payloads and a resource grant. */
    public static ArtifactBuildDispatchSpec of(
            String jobId, ArtifactStageGraph stageGraph,
            String tenantId, String principal, DeferredAuthorizationScope authorizationScope,
            ResourceGrant resources) {
        return new ArtifactBuildDispatchSpec(
                jobId, stageGraph,
                tenantId, principal, authorizationScope,
                resources.maxMemoryBytes(), resources.maxThreads(),
                resources.maxSpillBytes(), resources.taskTimeoutMillis());
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
