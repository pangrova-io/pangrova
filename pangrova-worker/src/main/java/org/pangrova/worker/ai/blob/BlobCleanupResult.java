package org.pangrova.worker.ai.blob;

/**
 * Result of a blob orphan cleanup operation for a single table.
 *
 * @param tableName      fully qualified table name
 * @param filesRemoved   number of orphaned blob files deleted
 * @param bytesReclaimed total bytes freed by deletion
 */
public record BlobCleanupResult(String tableName, int filesRemoved, long bytesReclaimed) {

    public BlobCleanupResult {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName must not be null or blank");
        }
        if (filesRemoved < 0) {
            throw new IllegalArgumentException("filesRemoved must be non-negative, got: " + filesRemoved);
        }
        if (bytesReclaimed < 0) {
            throw new IllegalArgumentException("bytesReclaimed must be non-negative, got: " + bytesReclaimed);
        }
    }
}
