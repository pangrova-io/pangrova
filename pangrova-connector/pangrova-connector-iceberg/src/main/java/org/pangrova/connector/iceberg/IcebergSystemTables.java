package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.system.IcebergFilesSystemTable;
import org.pangrova.connector.iceberg.system.IcebergHistorySystemTable;
import org.pangrova.connector.iceberg.system.IcebergManifestsSystemTable;
import org.pangrova.connector.iceberg.system.IcebergPartitionsSystemTable;
import org.pangrova.connector.iceberg.system.IcebergRefsSystemTable;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.iceberg.Table;

import java.util.Optional;

/**
 * Resolves Iceberg metadata (system) tables such as {@code snapshots},
 * {@code files}, and {@code history} for a given base table.
 */
final class IcebergSystemTables {

    private IcebergSystemTables() {}

    /** Resolves the named system table for the given Iceberg table, or empty if unknown. */
    static Optional<ConnectorSystemTable> resolve(Table icebergTable, String systemTableName) {
        return switch (systemTableName) {
            case "files" -> Optional.of(new IcebergFilesSystemTable(icebergTable));
            case "manifests" -> Optional.of(new IcebergManifestsSystemTable(icebergTable));
            case "partitions" -> Optional.of(new IcebergPartitionsSystemTable(icebergTable));
            case "history" -> Optional.of(new IcebergHistorySystemTable(icebergTable));
            case "refs" -> Optional.of(new IcebergRefsSystemTable(icebergTable));
            default -> Optional.empty();
        };
    }
}
