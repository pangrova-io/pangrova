package org.pangrova.spi.connector.index;

/**
 * Thrown when a referenced snapshot is no longer available in the connector's
 * snapshot history (e.g., expired by retention policy or compaction).
 */
public final class SnapshotExpiredException extends RuntimeException {

    private final String snapshotId;

    public SnapshotExpiredException(String snapshotId, String message) {
        super(message);
        this.snapshotId = snapshotId;
    }

    public String snapshotId() {
        return snapshotId;
    }
}
