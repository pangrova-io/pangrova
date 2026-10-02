package org.pangrova.metadata.artifact;

/** Exact immutable generated-value generation selected for a source snapshot. */
public record GeneratedValueArtifactReference(
        String tenantId,
        String artifactId,
        String generation,
        String sourceId,
        String sourceSnapshot,
        String targetColumn) {

    public GeneratedValueArtifactReference {
        requireText(tenantId, "tenantId");
        requireText(artifactId, "artifactId");
        requireText(generation, "generation");
        requireText(sourceId, "sourceId");
        requireText(sourceSnapshot, "sourceSnapshot");
        requireText(targetColumn, "targetColumn");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
