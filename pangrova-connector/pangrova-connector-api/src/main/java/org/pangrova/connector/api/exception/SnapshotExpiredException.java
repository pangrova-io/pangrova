package org.pangrova.connector.api.exception;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Thrown when a referenced snapshot is no longer available in the connector's
 * snapshot history (e.g., expired by retention policy or compaction).
 */
@Experimental
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
