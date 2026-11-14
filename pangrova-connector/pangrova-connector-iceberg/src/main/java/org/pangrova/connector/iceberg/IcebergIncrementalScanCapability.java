package org.pangrova.connector.iceberg;

import org.pangrova.connector.api.metadata.IncrementalScanCapability;
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
 * {@code fromSnapshotId} and requires each intermediate operation to be an
 * append without delete manifests. Overwrites can remove data files without
 * creating delete manifests.</p>
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
     * <p>Iceberg snapshot IDs are opaque positive identifiers; only parent
     * ancestry establishes the direction of an interval. Equal endpoints are
     * append-only only when that snapshot actually exists.</p>
     *
     * <p>For the actual range check, the method walks the snapshot
     * parent chain from {@code toSnapshotId} back to
     * {@code fromSnapshotId}, inspecting each operation and its delete manifests.</p>
     */
    @Override
    public boolean isAppendOnly(String tableName, long fromSnapshotId, long toSnapshotId) {
        if (fromSnapshotId <= 0 || toSnapshotId <= 0) {
            throw new IllegalArgumentException("Snapshot identifiers must be positive");
        }

        TableIdentifier tableId = parseTableIdentifier(tableName);
        Table table = catalog.loadTable(tableId);

        List<Snapshot> rangeSnapshots = collectSnapshotsInRange(
                table, fromSnapshotId, toSnapshotId);
        return containsOnlyAppends(table, rangeSnapshots);
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
     * Returns {@code true} only for append operations without delete manifests.
     * An overwrite can remove rows without producing a delete manifest.
     */
    private boolean containsOnlyAppends(Table table, List<Snapshot> snapshots) {
        for (Snapshot snapshot : snapshots) {
            if (!"append".equals(snapshot.operation()) || !snapshot.deleteManifests(table.io()).isEmpty()) {
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
