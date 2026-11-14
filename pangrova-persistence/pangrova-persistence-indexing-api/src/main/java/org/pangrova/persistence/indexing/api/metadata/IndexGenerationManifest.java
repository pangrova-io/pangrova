package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

import static org.pangrova.common.util.Preconditions.requireText;

/** Immutable aggregate identity published for one complete index generation. */
@Experimental
public record IndexGenerationManifest(
        String indexId,
        String sourceId,
        int generation,
        String sourceSnapshot,
        String artifactType,
        IndexGenerationFormat format,
        long objectCount,
        long rowCount,
        long totalBytes,
        String manifestSha256,
        long publicationEpoch,
        Instant publishedAt) {

    public IndexGenerationManifest {
        requireText(indexId, "indexId");
        requireText(sourceId, "sourceId");
        requireText(artifactType, "artifactType");
        requireCounts(generation, objectCount, rowCount, totalBytes);
        requirePublication(format, manifestSha256, publicationEpoch, publishedAt);
        sourceSnapshot = sourceSnapshot == null ? "" : sourceSnapshot;
    }

    private static void requireCounts(
            int generation, long objectCount, long rowCount, long totalBytes) {
        if (generation < 0 || objectCount < 0 || rowCount < 0 || totalBytes < 0) {
            throw new IllegalArgumentException(
                    "generation and aggregate counts must be non-negative");
        }
    }

    private static void requirePublication(
            IndexGenerationFormat format, String manifestSha256,
            long publicationEpoch, Instant publishedAt) {
        if (format == null || publishedAt == null) {
            throw new IllegalArgumentException("format and publishedAt are required");
        }
        if (!isSha256(manifestSha256)) {
            throw new IllegalArgumentException(
                    "manifestSha256 must be a SHA-256 hex digest");
        }
        if (publicationEpoch <= 0) {
            throw new IllegalArgumentException("publicationEpoch must be positive");
        }
    }

    private static boolean isSha256(String value) {
        return value != null && value.length() == 64
                && value.chars().allMatch(character -> character >= '0' && character <= '9'
                        || character >= 'a' && character <= 'f'
                        || character >= 'A' && character <= 'F');
    }
}
