package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.metadata.DeletionMode;
import org.apache.iceberg.Table;

/**
 * Resolves the Iceberg table format version (1, 2, or 3) from either the
 * table operations metadata or the {@code format-version} table property.
 */
final class IcebergFormatVersions {

    private IcebergFormatVersions() {}

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
