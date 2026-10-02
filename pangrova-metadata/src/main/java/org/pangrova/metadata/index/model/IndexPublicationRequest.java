package org.pangrova.metadata.index.model;

import java.time.Instant;

/** Compare-and-set request that atomically activates one complete index generation. */
public record IndexPublicationRequest(
        String indexId,
        String sourceId,
        int generation,
        String sourceSnapshot,
        String artifactType,
        IndexArtifactFormat format,
        long expectedObjectCount,
        Instant expectedUpdatedAt,
        Instant publishedAt) {

    public IndexPublicationRequest {
        requireIdentities(indexId, sourceId, artifactType);
        requireCounts(generation, expectedObjectCount);
        requirePublicationFields(format, expectedUpdatedAt, publishedAt);
        sourceSnapshot = sourceSnapshot == null ? "" : sourceSnapshot;
    }

    private static void requireIdentities(
            String indexId, String sourceId, String artifactType) {
        if (indexId == null || indexId.isBlank()
                || sourceId == null || sourceId.isBlank()
                || artifactType == null || artifactType.isBlank()) {
            throw new IllegalArgumentException(
                    "index, source, and artifact identities are required");
        }
    }

    private static void requireCounts(int generation, long expectedObjectCount) {
        if (generation < 0 || expectedObjectCount < 0) {
            throw new IllegalArgumentException("generation and object count must be non-negative");
        }
    }

    private static void requirePublicationFields(
            IndexArtifactFormat format, Instant expectedUpdatedAt, Instant publishedAt) {
        if (format == null || expectedUpdatedAt == null || publishedAt == null) {
            throw new IllegalArgumentException("format and publication timestamps are required");
        }
    }
}
