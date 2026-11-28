package org.pangrova.persistence.generation.api.job;

import org.pangrova.generation.api.job.GenerationSubmissionAuthority;
import org.pangrova.persistence.generation.api.enums.SnapshotPinState;

import java.time.Instant;
import java.util.Objects;

/** Immutable snapshot pin evidence retained across connector and job crash windows. */
public record SnapshotPinIntent(
        String tenantId,
        String indexId,
        int generation,
        String attemptId,
        String sourceId,
        String sourceSnapshot,
        GenerationSubmissionAuthority authority,
        SnapshotPinState state,
        String jobId,
        Instant createdAt,
        Instant updatedAt) {
    public SnapshotPinIntent {
        requireText(tenantId, "tenantId");
        requireText(indexId, "indexId");
        requireText(attemptId, "attemptId");
        requireText(sourceId, "sourceId");
        requireText(sourceSnapshot, "sourceSnapshot");
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        authority = Objects.requireNonNull(authority, "authority");
        if (!tenantId.equals(authority.tenantId())) {
            throw new IllegalArgumentException("snapshot intent tenant differs from its authority");
        }
        state = Objects.requireNonNull(state, "state");
        createdAt = Objects.requireNonNull(createdAt, "createdAt");
        updatedAt = Objects.requireNonNull(updatedAt, "updatedAt");
    }

    /** Returns the canonical owner shared with the connector SPI. */
    public String pinOwner() {
        return indexId + ":generation:" + generation + ":attempt:" + attemptId;
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
