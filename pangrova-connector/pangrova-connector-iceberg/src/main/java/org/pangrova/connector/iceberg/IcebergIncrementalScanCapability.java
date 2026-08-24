package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.metadata.IncrementalScanCapability;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Iceberg implementation of {@link IncrementalScanCapability}.
 *
 * <p>Iceberg supports incremental append scan via snapshot-diff but does
 * not natively produce a changelog stream. The {@link #isAppendOnly}
 * method walks the snapshot chain from {@code toSnapshotId} back to
 * {@code fromSnapshotId} and checks each intermediate snapshot for
 * delete manifests — their presence indicates non-append operations
 * (equality deletes, position deletes, or overwrites).</p>
 *
 * <p>Thread-safe: all state is final and Iceberg Catalog is safe for
 * concurrent reads.</p>
 */
public final class IcebergIncrementalScanCapability implements IncrementalScanCapability {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergIncrementalScanCapability.class);

    private final Catalog catalog;

    public IcebergIncrementalScanCapability(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public boolean supportsIncrementalAppendScan() {
        return true;
    }

    @Override
    public boolean supportsChangelogScan() {
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Iceberg snapshot IDs are non-sequential random longs, so the
     * numeric comparison {@code fromSnapshotId > toSnapshotId} serves
     * as a fast-fail contract check per the SPI specification. When
     * {@code fromSnapshotId == toSnapshotId}, no changes exist and the
     * method returns {@code true} without loading the table.</p>
     *
     * <p>For the actual range check, the method walks the snapshot
     * parent chain from {@code toSnapshotId} back to
     * {@code fromSnapshotId}, inspecting each snapshot for delete
     * manifests.</p>
     */
    @Override
    public boolean isAppendOnly(String tableName, long fromSnapshotId, long toSnapshotId) {
        if (fromSnapshotId > toSnapshotId) {
            throw new IllegalArgumentException(
                    "fromSnapshotId (" + fromSnapshotId
                            + ") must not be greater than toSnapshotId ("
                            + toSnapshotId + ")");
        }
        if (fromSnapshotId == toSnapshotId) {
            return true;
        }

        TableIdentifier tableId = parseTableIdentifier(tableName);
        Table table = catalog.loadTable(tableId);

        List<Snapshot> rangeSnapshots = collectSnapshotsInRange(
                table, fromSnapshotId, toSnapshotId);
        return hasNoDeleteManifests(table, rangeSnapshots);
    }

    /**
     * Collects snapshots between {@code fromSnapshotId} (exclusive) and
     * {@code toSnapshotId} (inclusive) by walking the parent chain from
     * {@code toSnapshotId} backwards.
     *
     * @throws IllegalArgumentException if {@code toSnapshotId} is not found,
     *         or {@code fromSnapshotId} is not an ancestor of {@code toSnapshotId}
     */
    private List<Snapshot> collectSnapshotsInRange(
            Table table, long fromSnapshotId, long toSnapshotId) {
        Snapshot start = table.snapshot(toSnapshotId);
        if (start == null) {
            throw new IllegalArgumentException("Snapshot not found: " + toSnapshotId);
        }
        SnapshotChainResult chain = walkSnapshotChain(table, start, fromSnapshotId);
        if (!chain.foundTarget()) {
            throw new IllegalArgumentException(
                    "fromSnapshotId (" + fromSnapshotId
                            + ") is not an ancestor of toSnapshotId (" + toSnapshotId + ")");
        }
        return chain.snapshots();
    }

    private SnapshotChainResult walkSnapshotChain(Table table, Snapshot start, long stopId) {
        List<Snapshot> snapshots = new ArrayList<>();
        Snapshot current = start;
        while (current != null && current.snapshotId() != stopId) {
            snapshots.add(current);
            Long parentId = current.parentId();
            current = (parentId != null) ? table.snapshot(parentId) : null;
        }
        boolean found = current != null && current.snapshotId() == stopId;
        return new SnapshotChainResult(snapshots, found);
    }

    private record SnapshotChainResult(List<Snapshot> snapshots, boolean foundTarget) { }

    /**
     * Returns {@code true} if none of the given snapshots contain
     * delete manifests.
     */
    private boolean hasNoDeleteManifests(Table table, List<Snapshot> snapshots) {
        for (Snapshot snapshot : snapshots) {
            if (!snapshot.deleteManifests(table.io()).isEmpty()) {
                LOG.debug("event=delete_manifests_found snapshot_id={} table={}",
                        snapshot.snapshotId(), table.name());
                return false;
            }
        }
        return true;
    }

    /**
     * Parses a fully qualified table name into an Iceberg
     * {@link TableIdentifier}.
     *
     * <p>Supports two formats:</p>
     * <ul>
     *   <li>{@code schema.table} — two-part name</li>
     *   <li>{@code catalog.schema.table} — three-part name
     *       (catalog prefix is stripped)</li>
     * </ul>
     */
    static TableIdentifier parseTableIdentifier(String tableName) {
        String[] parts = tableName.split("\\.");
        return switch (parts.length) {
            case 2 -> TableIdentifier.of(parts[0], parts[1]);
            case 3 -> TableIdentifier.of(parts[1], parts[2]);
            default -> throw new IllegalArgumentException(
                    "Invalid table name format: '" + tableName
                            + "'. Expected 'schema.table' or 'catalog.schema.table'");
        };
    }
}
