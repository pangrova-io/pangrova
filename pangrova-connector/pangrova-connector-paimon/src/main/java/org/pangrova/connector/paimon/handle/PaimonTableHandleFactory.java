package org.pangrova.connector.paimon.handle;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
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

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PaimonTableHandleFactory() {
        // Utility class
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
        Map<String, String> properties = table.options();
        String rowTypeJson = serializeRowType(table.rowType(), properties);
        List<String> primaryKeys = table.primaryKeys();
        List<String> partitionKeys = table.partitionKeys();

        return new PaimonTableHandle(
                schemaName,
                tableName,
                tableLocation,
                snapshotId,
                rowTypeJson,
                primaryKeys,
                partitionKeys,
                properties,
                PaimonTableHandle.NO_LIMIT);
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
        String tableLocation = extractLocation(table);
        String rowTypeJson = serializeRowType(table.rowType(), table.options());
        List<String> primaryKeys = table.primaryKeys();
        List<String> partitionKeys = table.partitionKeys();

        return new PaimonOutputTableHandle(
                schemaName,
                tableName,
                tableLocation,
                rowTypeJson,
                primaryKeys,
                partitionKeys);
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
        String tableLocation = extractLocation(table);
        String rowTypeJson = serializeRowType(table.rowType(), table.options());
        List<String> primaryKeys = table.primaryKeys();
        List<String> partitionKeys = table.partitionKeys();

        return new PaimonOutputTableHandle(
                schemaName,
                tableName,
                tableLocation,
                rowTypeJson,
                primaryKeys,
                partitionKeys,
                overwritePartition);
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
        // Paimon Table doesn't have a direct location() method like Iceberg.
        // We use the table name as identifier, and the actual location is
        // managed by the catalog. For serialization purposes, we construct
        // a logical path from the table's identity.
        String name = table.name();
        return name != null && !name.isBlank() ? name : "unknown";
    }

    /**
     * Serializes a Paimon RowType to JSON string.
     * Stores field metadata (id, name, type) as JSON array.
     */
    private static String serializeRowType(RowType rowType, Map<String, String> tableProperties) {
        List<FieldInfo> fields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            PangrovaType pangrovaType = PaimonTypeMapper.toPangrovaType(
                    field.type(), field.name(), tableProperties);
            fields.add(new FieldInfo(field.id(), field.name(), pangrovaType.name()));
        }
        try {
            return MAPPER.writeValueAsString(fields);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize RowType", e);
        }
    }

    /**
     * Deserializes a JSON string back to Paimon RowType.
     */
    private static RowType deserializeRowType(String rowTypeJson) {
        try {
            List<FieldInfo> fields = MAPPER.readValue(rowTypeJson, new TypeReference<>() {});
            List<DataField> dataFields = new ArrayList<>();
            for (FieldInfo info : fields) {
                PangrovaType pangrovaType = PangrovaType.valueOf(info.typeName());
                org.apache.paimon.types.DataType paimonType = PaimonTypeMapper.toPaimonType(pangrovaType);
                dataFields.add(new DataField(info.id(), info.name(), paimonType));
            }
            return new RowType(dataFields);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize RowType: " + rowTypeJson, e);
        }
    }

    /**
     * Internal record for JSON serialization of field metadata.
     */
    private record FieldInfo(int id, String name, String typeName) {}
}
