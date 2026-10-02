package org.pangrova.worker.jni.write;

import java.util.List;
import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Describes the target and mode of a native write operation.
 *
 * <p>Passed to {@link NativeWriter#beginWrite(WriteDescriptor)} to open
 * a write session against a specific table. The {@code properties} map carries
 * connector-specific options (e.g., Iceberg copy-on-write vs merge-on-read,
 * partition spec overrides) without coupling the bridge to any single connector.
 *
 * @param catalogName  catalog that owns the target table
 * @param schemaName   schema that owns the target table
 * @param tableName    unqualified target table name
 * @param dmlOperation semantic DML operation (insert, overwrite, or delete)
 * @param columns      column schema of the data being written
 * @param properties   connector-specific options (never null, may be empty)
 */
public record WriteDescriptor(
        String catalogName,
        String schemaName,
        String tableName,
        DmlOperation dmlOperation,
        List<ColumnSchema> columns,
        Map<String, String> properties) {

    public static final String FILE_FORMAT_PROPERTY = "fileFormat";
    public static final String TARGET_FILE_SIZE_PROPERTY = "targetFileSizeBytes";

    /**
     * Compact constructor enforcing non-null invariants and defensive copies.
     */
    public WriteDescriptor {
        checkNotNull(catalogName, "catalogName");
        checkNotNull(schemaName, "schemaName");
        checkNotNull(tableName, "tableName");
        checkNotNull(dmlOperation, "dmlOperation");
        columns = columns != null ? List.copyOf(columns) : List.of();
        properties = properties != null ? Map.copyOf(properties) : Map.of();
    }

    /**
     * Semantic DML operation for native write operations.
     *
     * <p>Distinct from the connector-level
     * {@link org.pangrova.spi.connector.data.write.WriteMode} which models
     * append/upsert/overwrite semantics at the storage layer. This enum
     * models the DML operation intent as seen by the native execution engine.
     */
    public enum DmlOperation {
        /** Insert new rows into the target table. */
        INSERT,
        /** Overwrite existing rows (used by UPDATE and full-table MERGE). */
        OVERWRITE,
        /** Delete rows matching a predicate. */
        DELETE
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
