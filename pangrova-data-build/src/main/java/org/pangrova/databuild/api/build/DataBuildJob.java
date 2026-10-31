package org.pangrova.databuild.api.build;

import org.pangrova.databuild.api.DataBuildDependencySet;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildExecutionKind;

import java.time.Instant;
import java.util.Objects;

/** Immutable identity plus mutable progress snapshot for one artifact generation. */
public record DataBuildJob(
        String tenantId,
        String jobId,
        String artifactId,
        String artifactType,
        String stagingGeneration,
        String sourceId,
        String sourceSnapshot,
        DataBuildExecutionKind executionKind,
        DataBuildDependencySet dependencySet,
        DataBuildEmptyPartitionPolicy emptyPartitionPolicy,
        String stagingUri,
        String formatId,
        int formatMajor,
        int formatMinor,
        String schemaSha256,
        String writerBuildId,
        String encryptionKeyRef,
        String idempotencyKey,
        int expectedPages,
        int completedPages,
        DataBuildJobState state,
        long publicationFencingToken,
        Instant publicationExpectedUpdatedAt,
        Instant createdAt,
        Instant updatedAt) {

    public DataBuildJob {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        requireText(artifactId, "artifactId");
        requireText(artifactType, "artifactType");
        requireText(stagingGeneration, "stagingGeneration");
        requireText(sourceId, "sourceId");
        requireText(sourceSnapshot, "sourceSnapshot");
        executionKind = Objects.requireNonNull(executionKind, "executionKind must not be null");
        dependencySet = Objects.requireNonNull(dependencySet, "dependencySet must not be null");
        emptyPartitionPolicy = Objects.requireNonNull(
                emptyPartitionPolicy, "emptyPartitionPolicy must not be null");
        if (dependencySet.dependencies().stream().noneMatch(dependency ->
                dependency.kind().equals("SOURCE_SNAPSHOT")
                        && dependency.id().equals(sourceId)
                        && dependency.version().equals(sourceSnapshot))) {
            throw new IllegalArgumentException(
                    "artifact dependencies must pin the primary source snapshot");
        }
        requireText(stagingUri, "stagingUri");
        requireText(formatId, "formatId");
        requireText(schemaSha256, "schemaSha256");
        requireText(writerBuildId, "writerBuildId");
        requireText(idempotencyKey, "idempotencyKey");
        encryptionKeyRef = Objects.requireNonNull(
                encryptionKeyRef, "encryptionKeyRef must not be null");
        state = Objects.requireNonNull(state, "state must not be null");
        publicationExpectedUpdatedAt = Objects.requireNonNull(
                publicationExpectedUpdatedAt,
                "publicationExpectedUpdatedAt must not be null");
        createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        updatedAt = Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (formatMajor <= 0 || formatMinor < 0) {
            throw new IllegalArgumentException(
                    "format major must be positive and minor must be non-negative");
        }
        if (!schemaSha256.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException("schemaSha256 must be 64 hexadecimal characters");
        }
        if (expectedPages < 0 || completedPages < 0 || completedPages > expectedPages) {
            throw new IllegalArgumentException("invalid artifact page counters");
        }
        if (publicationFencingToken < 0) {
            throw new IllegalArgumentException("publicationFencingToken must be non-negative");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
