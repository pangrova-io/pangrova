package org.pangrova.coordinator.query.execution.dispatch.dag.retrieval;

import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.NamedValue;

import java.util.List;

/**
 * Query-scoped intent to consume one immutable artifact generation.
 * Physical objects are deliberately absent: they are resolved only after
 * task partitioning and only for that task's source object identities.
 */
public record RetrievalGenerationIntent(
        String tenantId,
        String planSourceKey,
        String artifactId,
        String artifactType,
        String generation,
        String sourceId,
        String sourceSnapshot,
        CapabilityRef capability,
        List<NamedValue> invocationOptions,
        int candidateLimit) {

    public RetrievalGenerationIntent {
        requireText(tenantId, "tenantId");
        requireText(planSourceKey, "planSourceKey");
        requireText(artifactId, "artifactId");
        requireText(artifactType, "artifactType");
        requireText(generation, "generation");
        requireText(sourceId, "sourceId");
        requireText(sourceSnapshot, "sourceSnapshot");
        if (capability == null || capability.getCapabilityId().isBlank()
                || capability.getSemanticVersion().isBlank()
                || capability.getSignatureSha256().size() != 32) {
            throw new IllegalArgumentException("capability identity is incomplete");
        }
        if (candidateLimit <= 0
                || candidateLimit
                        > org.pangrova.protocol.retrieval.RetrievalMergeBinding
                                .MAX_CANDIDATE_ROWS) {
            throw new IllegalArgumentException("candidateLimit is outside the bounded range");
        }
        invocationOptions = List.copyOf(invocationOptions);
        java.util.HashSet<String> names = new java.util.HashSet<>();
        if (invocationOptions.stream().anyMatch(option -> option.getName().isBlank()
                || !option.hasValue() || !names.add(option.getName()))) {
            throw new IllegalArgumentException(
                    "invocation options are blank, duplicated or untyped");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
