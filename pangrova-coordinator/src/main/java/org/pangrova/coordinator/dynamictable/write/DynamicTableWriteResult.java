package org.pangrova.coordinator.dynamictable.write;

import java.util.Objects;

/**
 * Immutable result of writing incremental data to a Dynamic Table target table.
 *
 * @param bytesWritten  total bytes written to the target table
 * @param rowsWritten   total rows written to the target table
 * @param targetTable   fully qualified target table name (e.g. {@code __dt_NAME})
 * @param checkpoint    source checkpoint after this write (e.g. snapshot ID or CDC offset)
 */
public record DynamicTableWriteResult(
        long bytesWritten,
        long rowsWritten,
        String targetTable,
        String checkpoint
) {

    public DynamicTableWriteResult {
        Objects.requireNonNull(targetTable, "targetTable must not be null");
        if (bytesWritten < 0) {
            throw new IllegalArgumentException("bytesWritten must be non-negative");
        }
        if (rowsWritten < 0) {
            throw new IllegalArgumentException("rowsWritten must be non-negative");
        }
    }
}
