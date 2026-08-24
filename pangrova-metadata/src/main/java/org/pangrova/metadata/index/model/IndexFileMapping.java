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
 * @param checksum        CRC32 checksum of the index file content
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
        if (indexId == null || indexId.isBlank()) {
            throw new IllegalArgumentException("indexId must not be null or blank");
        }
        if (dataFilePath == null || dataFilePath.isBlank()) {
            throw new IllegalArgumentException("dataFilePath must not be null or blank");
        }
        if (indexFilePath == null || indexFilePath.isBlank()) {
            throw new IllegalArgumentException("indexFilePath must not be null or blank");
        }
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt must not be null");
        }
    }
}
