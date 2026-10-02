package org.pangrova.metadata.artifact;

import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;

import java.time.Instant;

/** Canonical immutable publication view shared by every artifact consumer. */
public record ArtifactGenerationManifest(
        String tenantId,
        String jobId,
        String artifactId,
        String artifactType,
        String generation,
        String sourceId,
        String sourceSnapshot,
        ArtifactExecutionKind executionKind,
        ArtifactDependencySet dependencySet,
        ArtifactEmptyPartitionPolicy emptyPartitionPolicy,
        String formatId,
        int formatMajor,
        int formatMinor,
        String schemaSha256,
        String writerBuildId,
        String manifestUri,
        String manifestSha256,
        long objectCount,
        long rowCount,
        long totalBytes,
        Instant publishedAt) {

    public ArtifactGenerationManifest {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        requireText(artifactId, "artifactId");
        requireText(artifactType, "artifactType");
        requireText(generation, "generation");
        requireText(sourceId, "sourceId");
        requireText(sourceSnapshot, "sourceSnapshot");
        if (executionKind == null || dependencySet == null || emptyPartitionPolicy == null) {
            throw new IllegalArgumentException(
                    "artifact execution, dependency and empty-partition metadata is required");
        }
        if (dependencySet.dependencies().stream().noneMatch(dependency ->
                dependency.kind().equals("SOURCE_SNAPSHOT")
                        && dependency.id().equals(sourceId)
                        && dependency.version().equals(sourceSnapshot))) {
            throw new IllegalArgumentException(
                    "artifact manifest dependencies do not pin its primary source snapshot");
        }
        requireText(formatId, "formatId");
        requireText(writerBuildId, "writerBuildId");
        requireText(manifestUri, "manifestUri");
        requireSha(schemaSha256, "schemaSha256");
        requireSha(manifestSha256, "manifestSha256");
        if (formatMajor <= 0 || formatMinor < 0
                || objectCount < 0 || rowCount < 0 || totalBytes < 0) {
            throw new IllegalArgumentException("artifact manifest counters or versions are invalid");
        }
        if (publishedAt == null) {
            throw new IllegalArgumentException("publishedAt must not be null");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static void requireSha(String value, String name) {
        if (value == null || !value.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException(name + " must be a SHA-256 hex digest");
        }
    }
}
