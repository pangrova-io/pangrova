package org.pangrova.spi.connector.index;

import org.pangrova.spi.annotation.Experimental;

/** Immutable identity and physical statistics for one file in a table snapshot. */
@Experimental
public record SnapshotDataFile(
        String path,
        long rowCount,
        long sizeBytes,
        String format) {

    public SnapshotDataFile {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path must not be blank");
        }
        if (rowCount < 0 || sizeBytes < 0) {
            throw new IllegalArgumentException("rowCount and sizeBytes must be non-negative");
        }
        if (format == null || format.isBlank()) {
            throw new IllegalArgumentException("format must not be blank");
        }
    }
}
