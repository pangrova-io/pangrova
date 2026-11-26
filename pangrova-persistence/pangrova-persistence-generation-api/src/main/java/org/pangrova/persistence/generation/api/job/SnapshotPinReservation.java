package org.pangrova.persistence.generation.api.job;

import org.pangrova.generation.api.job.GenerationSubmissionAuthority;

import java.time.Instant;
import java.util.Objects;

/** Authorized durable intent recorded before a connector snapshot mutation. */
public record SnapshotPinReservation(
        String tenantId,
        String indexId,
        int generation,
        String attemptId,
        String sourceId,
        String sourceSnapshot,
        GenerationSubmissionAuthority authority,
        Instant createdAt) {
    public SnapshotPinReservation {
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
        createdAt = Objects.requireNonNull(createdAt, "createdAt");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
