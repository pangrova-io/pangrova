package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.metadata.DeletionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.pangrova.connector.iceberg.schema.IcebergSchemaBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves the Iceberg table format version (1, 2, or 3) from either the
 * table operations metadata or the {@code format-version} table property.
 */
final class IcebergFormatVersions {

    private IcebergFormatVersions() {}

    /** Selects v3 for new nanosecond schemas without changing an existing table. */
    static Map<String, String> creationProperties(Schema schema, Map<String, String> properties) {
        Map<String, String> result = IcebergSchemaBuilder.extractIcebergTableProperties(properties);
        if (!containsNanoTimestamp(schema.asStruct())) {
            return result;
        }
        String version = result.get("format-version");
        if (version != null) {
            if (Integer.parseInt(version) < 3) {
                throw new IllegalArgumentException("TIMESTAMP_NS requires Iceberg format-version >= 3");
            }
            return result;
        }
        Map<String, String> nanoProperties = new LinkedHashMap<>(result);
        nanoProperties.put("format-version", "3");
        return Map.copyOf(nanoProperties);
    }

    private static boolean containsNanoTimestamp(Type type) {
        return switch (type.typeId()) {
            case TIMESTAMP_NANO -> true;
            case STRUCT -> type.asStructType().fields().stream()
                    .anyMatch(field -> containsNanoTimestamp(field.type()));
            case LIST -> containsNanoTimestamp(type.asListType().elementType());
            case MAP -> containsNanoTimestamp(type.asMapType().keyType())
                    || containsNanoTimestamp(type.asMapType().valueType());
            default -> false;
        };
    }

    /** Returns the format version of the given Iceberg table, defaulting to 1. */
    static int extract(Table table) {
        if (table instanceof org.apache.iceberg.HasTableOperations hasOps) {
            return hasOps.operations().current().formatVersion();
        }
        String versionStr = table.properties().get("format-version");
        if (versionStr != null) {
            try {
                return Integer.parseInt(versionStr);
            } catch (NumberFormatException e) {
                return 1;
            }
        }
        return 1;
    }

    /** Maps the table format version to the connector deletion mode. */
    static DeletionMode deletionMode(Table table) {
        int formatVersion = extract(table);
        if (formatVersion >= 3) {
            return DeletionMode.DELETION_VECTOR;
        }
        if (formatVersion >= 2) {
            return DeletionMode.POSITION_DELETE;
        }
        return DeletionMode.COPY_ON_WRITE;
    }
}
