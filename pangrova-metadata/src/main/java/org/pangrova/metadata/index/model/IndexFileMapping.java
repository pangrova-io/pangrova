package org.pangrova.metadata.index.model;

import java.time.Instant;

/**
 * Tracks the mapping between a data file and its corresponding index file.
 *
 * <p>Each mapping records which data file has been indexed, where the index
 * file is stored, and metadata for GC, snapshot consistency, and verification.</p>
 *
 * @param indexId         owning index identifier
 * @param dataFilePath    path to the original data file
 * @param indexFilePath   path to the generated index file
 * @param rowCount        number of rows indexed from the data file
 * @param indexSizeBytes  size of the index file in bytes
 * @param checksum        SHA-256 checksum of the index file content
 * @param generation      rebuild generation at time of creation
 * @param snapshotToken   snapshot identifier for consistency filtering
 * @param sequenceNum     ordering sequence within a snapshot
 * @param createdAt       creation timestamp
 * @param lastVerifiedAt  epoch millis of last GC verification scan
 */
public record IndexFileMapping(
        String indexId,
        String dataFilePath,
        String indexFilePath,
        long rowCount,
        long indexSizeBytes,
        String checksum,
        int generation,
        String snapshotToken,
        long sequenceNum,
        Instant createdAt,
        long lastVerifiedAt) {

    public IndexFileMapping {
        requireIdentities(indexId, dataFilePath, indexFilePath);
        requireCounts(rowCount, indexSizeBytes, generation, sequenceNum, lastVerifiedAt);
        requireChecksum(checksum);
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt must not be null");
        }
        snapshotToken = snapshotToken == null ? "" : snapshotToken;
    }

    private static void requireIdentities(
            String indexId, String dataFilePath, String indexFilePath) {
        if (indexId == null || indexId.isBlank()) {
            throw new IllegalArgumentException("indexId must not be null or blank");
        }
        if (dataFilePath == null || dataFilePath.isBlank()) {
            throw new IllegalArgumentException("dataFilePath must not be null or blank");
        }
        if (indexFilePath == null || indexFilePath.isBlank()) {
            throw new IllegalArgumentException("indexFilePath must not be null or blank");
        }
    }

    private static void requireCounts(
            long rowCount, long indexSizeBytes, int generation,
            long sequenceNum, long lastVerifiedAt) {
        if (rowCount < 0 || indexSizeBytes < 0 || generation < 0
                || sequenceNum < 0 || lastVerifiedAt < 0) {
            throw new IllegalArgumentException(
                    "mapping counts, generation, sequence and verification time must be non-negative");
        }
    }

    private static void requireChecksum(String checksum) {
        if (checksum == null || checksum.length() != 64
                || !checksum.chars().allMatch(IndexFileMapping::isHexDigit)) {
            throw new IllegalArgumentException(
                    "checksum must be a 64-character SHA-256 hex digest");
        }
    }

    private static boolean isHexDigit(int value) {
        return value >= '0' && value <= '9'
                || value >= 'a' && value <= 'f'
                || value >= 'A' && value <= 'F';
    }
}
