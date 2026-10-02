package org.pangrova.metadata.artifact;

import java.time.Instant;

/** Durable identity of one generated-column sidecar independent of its generations. */
public record GeneratedValueArtifactDescriptor(
        String tenantId,
        String artifactId,
        String sourceId,
        String catalogName,
        String schemaName,
        String tableName,
        String targetColumn,
        String definitionSha256,
        String targetGeneration,
        String targetSourceSnapshot,
        Instant updatedAt) {

    public GeneratedValueArtifactDescriptor {
        requireText(tenantId, "tenantId");
        requireText(artifactId, "artifactId");
        requireText(sourceId, "sourceId");
        requireText(catalogName, "catalogName");
        requireText(schemaName, "schemaName");
        requireText(tableName, "tableName");
        requireText(targetColumn, "targetColumn");
        if (definitionSha256 == null || !definitionSha256.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException("definitionSha256 must be hexadecimal SHA-256");
        }
        requireText(targetGeneration, "targetGeneration");
        requireText(targetSourceSnapshot, "targetSourceSnapshot");
        if (updatedAt == null) {
            throw new IllegalArgumentException("updatedAt must not be null");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
