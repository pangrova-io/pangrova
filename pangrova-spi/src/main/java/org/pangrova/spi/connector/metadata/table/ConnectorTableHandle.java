package org.pangrova.spi.connector.metadata.table;

import org.pangrova.spi.annotation.Experimental;

/**
 * Marker interface for connector-specific table handle.
 *
 * <p>Each connector implementation provides its own class implementing this interface
 * to represent a reference to a table in the underlying data source. The handle is
 * opaque to the engine and only meaningful to the connector that created it.</p>
 */
@Experimental
public interface ConnectorTableHandle {

    /**
     * Returns a new handle pinned to the specified snapshot for time-travel queries.
     *
     * <p>Connectors that support snapshot-based time travel (e.g. Iceberg, Paimon)
     * override this method to return a handle that scans at the given snapshot.
     * The default returns {@code this} unchanged for connectors without time travel.</p>
     *
     * @param snapshotId the snapshot ID to pin the scan to
     * @return a new handle for time-travel, or this handle if unsupported
     */
    default ConnectorTableHandle withTimeTravelSnapshot(long snapshotId) {
        return this;
    }

    /**
     * Returns a new handle that reads only rows appended inside a snapshot range.
     *
     * <p>Only connectors that can serve an incremental change set implement this
     * method. The default fails closed so a planner mistake can never silently
     * turn an incremental refresh into a full scan.
     *
     * @param range inclusive append-only snapshot range
     * @return a handle bound to the incremental range
     */
    default ConnectorTableHandle withIncrementalRange(SnapshotRange range) {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support incremental range reads");
    }

    /**
     * Returns the incremental range bound to this handle.
     *
     * @return bound range, or empty for ordinary reads
     */
    default java.util.Optional<SnapshotRange> incrementalRange() {
        return java.util.Optional.empty();
    }

    /**
     * Returns the partition key column names for this table.
     *
     * <p>Used by the coordinator's copy-on-write DELETE path to perform
     * per-partition overwrites, ensuring that partitions with all rows
     * deleted are correctly emptied rather than left untouched.</p>
     *
     * @return partition key column names, empty list if not partitioned
     */
    default java.util.List<String> partitionKeys() {
        return java.util.List.of();
    }

    /**
     * Returns the format identity captured when this handle was created.
     *
     * <p>Advisory only: a write or maintenance operation must re-check the live
     * identity through the metadata SPI before acting, because the name may have
     * been dropped and recreated since planning.</p>
     */
    default java.util.Optional<TableIdentity> tableIdentity() {
        return java.util.Optional.empty();
    }

    /** Returns capabilities declared by the handle; undeclared means unsupported. */
    default TableCapabilities capabilities() {
        return TableCapabilities.UNSUPPORTED;
    }
}
