package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

import java.util.Map;
import java.util.Optional;

/**
 * Immutable context for Iceberg write operations.
 * Groups the common parameters shared by all Iceberg page sinks.
 *
 * <p>Encryption properties (if present) are pre-resolved by the coordinator
 * and passed as opaque Parquet configuration entries. The connector does not
 * interact with key management directly.
 */
public record IcebergWriteContext(
        Table table,
        Schema writeSchema,
        String schemaName,
        String tableName,
        Optional<Map<String, String>> encryptionProperties
) {

    /**
     * Convenience constructor for non-encrypted writes.
     */
    public IcebergWriteContext(Table table, Schema writeSchema, String schemaName, String tableName) {
        this(table, writeSchema, schemaName, tableName, Optional.empty());
    }
}
