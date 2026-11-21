package org.pangrova.worker.write;

import org.pangrova.worker.enums.DmlOperation;

import java.util.List;
import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Describes the target and mode of a native write operation.
 *
 * <p>Passed to {@link NativeWriter#beginWrite(WriteDescriptor)} to open
 * a write session against its task-bound connector output handle. The {@code properties} map carries
 * connector-specific options (e.g., Iceberg copy-on-write vs merge-on-read,
 * partition spec overrides) for the selected connector.
 *
 * @param dmlOperation semantic DML operation (insert, overwrite, or delete)
 * @param columns      column schema of the data being written
 * @param properties   connector-specific options (never null, may be empty)
 */
public record WriteDescriptor(
        DmlOperation dmlOperation,
        List<ColumnSchema> columns,
        Map<String, String> properties) {

    public static final String FILE_FORMAT_PROPERTY = "fileFormat";
    public static final String TARGET_FILE_SIZE_PROPERTY = "targetFileSizeBytes";

    /**
     * Compact constructor enforcing non-null invariants and defensive copies.
     */
    public WriteDescriptor {
        checkNotNull(dmlOperation, "dmlOperation");
        columns = columns != null ? List.copyOf(columns) : List.of();
        properties = properties != null ? Map.copyOf(properties) : Map.of();
    }

    /**
     * Schema of a single column in the write target.
     *
     * @param name column name
     * @param type column type descriptor
     */
    public record ColumnSchema(String name, String type) {
        public ColumnSchema {
            checkNotNull(name, "name");
            checkNotNull(type, "type");
        }
    }
}
