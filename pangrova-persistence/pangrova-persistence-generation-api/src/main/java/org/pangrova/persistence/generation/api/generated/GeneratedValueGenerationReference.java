package org.pangrova.persistence.generation.api.generated;

import static org.pangrova.common.validation.Preconditions.requireText;

/** Exact immutable generated-value generation selected for a source snapshot. */
public record GeneratedValueGenerationReference(
        String tenantId,
        String artifactId,
        String generation,
        String sourceId,
        String sourceSnapshot,
        String targetColumn) {

    public GeneratedValueGenerationReference {
        requireText(tenantId, "tenantId");
        requireText(artifactId, "artifactId");
        requireText(generation, "generation");
        requireText(sourceId, "sourceId");
        requireText(sourceSnapshot, "sourceSnapshot");
        requireText(targetColumn, "targetColumn");
    }
}
