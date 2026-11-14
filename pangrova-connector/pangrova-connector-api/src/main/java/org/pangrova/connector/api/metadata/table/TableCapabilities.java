package org.pangrova.connector.api.metadata.table;

import org.pangrova.connector.api.enums.WriterFenceKind;
import org.pangrova.common.api.annotation.Experimental;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Capabilities a connector can prove for a table format.
 *
 * <p>Undeclared means unsupported: the default is {@link #UNSUPPORTED}, and no
 * capability may be inferred from catalog type or from the presence of a write
 * path. Callers must reject an operation whose capability is false rather than
 * attempting it and falling back.</p>
 *
 * @param supportsWrite                  the connector can commit new data
 * @param supportsDelete                 the connector can commit row deletion
 * @param supportsEqualityDeleteRead     reads apply format equality deletes
 * @param supportsSnapshotIsolation      a scan is pinned to one table version
 * @param supportsAbort                  an uncommitted write can be rolled back
 * @param supportsStatistics             durable row/column statistics are available
 * @param systemTables                   system-table suffixes the connector serves
 * @param allowedMaintenanceOperations   maintenance operations the format can execute
 * @param writerFenceKind                evidence a write must carry to outlive a drop/recreate
 */
@Experimental
public record TableCapabilities(
        boolean supportsWrite,
        boolean supportsDelete,
        boolean supportsEqualityDeleteRead,
        boolean supportsSnapshotIsolation,
        boolean supportsAbort,
        boolean supportsStatistics,
        Set<String> systemTables,
        Set<String> allowedMaintenanceOperations,
        WriterFenceKind writerFenceKind) {


    /** Fail-closed default: nothing is supported. */
    public static final TableCapabilities UNSUPPORTED = new TableCapabilities(
            false, false, false, false, false, false,
            Set.of(), Set.of(), WriterFenceKind.NONE);

    public TableCapabilities {
        systemTables = systemTables == null ? Set.of() : Set.copyOf(systemTables);
        allowedMaintenanceOperations = allowedMaintenanceOperations == null
                ? Set.of() : Set.copyOf(allowedMaintenanceOperations);
        Objects.requireNonNull(writerFenceKind, "writerFenceKind");
    }

    /** Returns whether the suffix names a system table this connector implements. */
    public boolean supportsSystemTable(String suffix) {
        if (suffix == null || suffix.isBlank()) {
            return false;
        }
        return systemTables.contains(suffix.trim().toLowerCase(Locale.ROOT));
    }

    /** Returns whether the named maintenance operation is executable by this format. */
    public boolean allowsMaintenanceOperation(String operation) {
        if (operation == null || operation.isBlank()) {
            return false;
        }
        return allowedMaintenanceOperations.contains(operation.trim().toUpperCase(Locale.ROOT));
    }
}
