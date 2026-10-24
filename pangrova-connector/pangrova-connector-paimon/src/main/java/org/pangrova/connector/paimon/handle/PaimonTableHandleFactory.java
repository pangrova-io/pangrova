package org.pangrova.connector.paimon.handle;

import org.pangrova.protocol.GeneratedColumnMetadataCodec;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Factory for creating immutable {@link PaimonTableHandle} and {@link PaimonOutputTableHandle}
 * instances from Paimon SDK objects.
 *
 * <p>This factory extracts immutable metadata snapshots from live Paimon Table objects,
 * ensuring that handles contain only serializable, immutable data.
 */
public final class PaimonTableHandleFactory {

    private PaimonTableHandleFactory() {
        // Utility class
    }

    /** Rejects generated writes until the SDK has a verified declaration-and-snapshot atomic gate. */
    public static void requireWriteDeclarationSupported(Map<String, String> properties) {
        if (GeneratedColumnMetadataCodec.decode(properties).getDefinitionsCount() != 0) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Paimon generated writes require an atomic declaration-and-snapshot commit gate; "
                            + "the SDK expiring catalog lock does not provide declaration fencing");
        }
    }

    // ========== PaimonTableHandle Factory Methods ==========

    /**
     * Creates an immutable table handle from a Paimon SDK Table object.
     *
     * @param schemaName the schema (database) name
     * @param tableName the table name
     * @param table the live Paimon SDK Table object
     * @return an immutable handle containing metadata snapshot
     */
    public static PaimonTableHandle fromTable(String schemaName, String tableName, Table table) {
        return fromTable(schemaName, tableName, table, Optional.empty());
    }

    /**
     * Creates an immutable table handle from a Paimon SDK Table object with optional snapshot.
     *
     * @param schemaName the schema (database) name
     * @param tableName the table name
     * @param table the live Paimon SDK Table object
     * @param snapshotId optional snapshot ID for time-travel queries
     * @return an immutable handle containing metadata snapshot
     */
    public static PaimonTableHandle fromTable(
            String schemaName,
            String tableName,
            Table table,
            Optional<Long> snapshotId) {
        String tableLocation = extractLocation(table);
        String tableUuid = extractUuid(table);
        String schemaFingerprint = PaimonTableIdentity.schemaFingerprint(
                requireFileStore(table).schema());
        Map<String, String> properties = table.options();
        String rowTypeJson = JsonSerdeUtil.toFlatJson(table.rowType());
        List<String> primaryKeys = table.primaryKeys();
        List<String> partitionKeys = table.partitionKeys();

        return new PaimonTableHandle(
                schemaName,
                tableName,
                tableLocation,
                tableUuid,
                schemaFingerprint,
                snapshotId,
                rowTypeJson,
                primaryKeys,
                partitionKeys,
                properties,
                PaimonTableHandle.NO_LIMIT,
                Optional.empty());
    }

    /**
     * Creates a handle with a specific snapshot ID.
     *
     * @param schemaName the schema (database) name
     * @param tableName the table name
     * @param table the live Paimon SDK Table object
     * @param snapshotId the snapshot ID for time-travel
     * @return an immutable handle with the specified snapshot
     */
    public static PaimonTableHandle fromTableWithSnapshot(
            String schemaName,
            String tableName,
            Table table,
            long snapshotId) {
        return fromTable(schemaName, tableName, table, Optional.of(snapshotId));
    }

    // ========== PaimonOutputTableHandle Factory Methods ==========

    /**
     * Creates an immutable output table handle from a Paimon SDK Table object.
     *
     * @param schemaName the schema (database) name
     * @param tableName the table name
     * @param table the live Paimon SDK Table object
     * @return an immutable output handle containing metadata snapshot
     */
    public static PaimonOutputTableHandle outputFromTable(
            String schemaName,
            String tableName,
            Table table) {
        return outputFromTable(schemaName, tableName, table, Map.of(), "INSERT");
    }

    public static PaimonOutputTableHandle outputFromTable(
            String schemaName,
            String tableName,
            Table table,
            String mode) {
        return outputFromTable(schemaName, tableName, table, Map.of(), mode);
    }

    private static PaimonOutputTableHandle outputFromTable(
            String schemaName,
            String tableName,
            Table table,
            Map<String, String> overwritePartition,
            String mode) {
        requireWriteDeclarationSupported(table.options());
        String tableLocation = extractLocation(table);
        String tableUuid = extractUuid(table);
        String schemaFingerprint = PaimonTableIdentity.schemaFingerprint(
                requireFileStore(table).schema());
        String rowTypeJson = JsonSerdeUtil.toFlatJson(table.rowType());
        List<String> primaryKeys = table.primaryKeys();
        List<String> partitionKeys = table.partitionKeys();
        Long latestSnapshot = table instanceof FileStoreTable fileStore
                ? fileStore.snapshotManager().latestSnapshotId() : null;

        return new PaimonOutputTableHandle(
                schemaName,
                tableName,
                tableLocation,
                tableUuid,
                schemaFingerprint,
                rowTypeJson,
                new PaimonOutputTableHandle.Layout(primaryKeys, partitionKeys),
                new PaimonOutputTableHandle.WriteSemantics(overwritePartition, mode,
                        latestSnapshot == null ? -1L : latestSnapshot, Optional.empty()));
    }

    /**
     * Creates an immutable output table handle with a static partition filter for overwrite.
     *
     * @param schemaName         the schema (database) name
     * @param tableName          the table name
     * @param table              the live Paimon SDK Table object
     * @param overwritePartition partition column name-value pairs for static overwrite
     * @return an immutable output handle with partition filter
     */
    public static PaimonOutputTableHandle outputFromTable(
            String schemaName,
            String tableName,
            Table table,
            Map<String, String> overwritePartition) {
        return outputFromTable(schemaName, tableName, table, overwritePartition, "OVERWRITE");
    }

    // ========== RowType Parsing ==========

    /**
     * Parses the row type JSON back to a Paimon RowType object.
     * Useful for consumers that need the schema without loading the full table.
     *
     * @param handle the table handle containing row type JSON
     * @return the parsed Paimon RowType
     */
    public static RowType parseRowType(PaimonTableHandle handle) {
        return deserializeRowType(handle.rowTypeJson());
    }

    /**
     * Parses the row type JSON from an output handle back to a Paimon RowType object.
     *
     * @param handle the output table handle containing row type JSON
     * @return the parsed Paimon RowType
     */
    public static RowType parseRowType(PaimonOutputTableHandle handle) {
        return deserializeRowType(handle.rowTypeJson());
    }

    // ========== Helper Methods ==========

    /**
     * Extracts the table location from a Paimon table.
     * Falls back to table name if location is not available.
     */
    private static String extractLocation(Table table) {
        return requireFileStore(table).location().toString();
    }

    private static String extractUuid(Table table) {
        String uuid = requireFileStore(table).uuid();
        if (uuid == null || uuid.isBlank()) {
            throw new IllegalArgumentException("Paimon table handle requires a stable SDK table UUID");
        }
        return uuid;
    }

    private static FileStoreTable requireFileStore(Table table) {
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            throw new IllegalArgumentException(
                    "Paimon table handle requires a physical FileStoreTable location");
        }
        return fileStoreTable;
    }

    /**
     * Deserializes a JSON string back to Paimon RowType.
     */
    private static RowType deserializeRowType(String rowTypeJson) {
        DataType type = JsonSerdeUtil.fromJson(rowTypeJson, DataType.class);
        if (!(type instanceof RowType row)) {
            throw new IllegalArgumentException("Paimon table handle requires a complete SDK ROW schema");
        }
        return row;
    }
}
