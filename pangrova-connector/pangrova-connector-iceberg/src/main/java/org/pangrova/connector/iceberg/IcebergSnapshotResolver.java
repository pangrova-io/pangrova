package org.pangrova.connector.iceberg;

import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Resolves snapshot-level metadata: commit timestamps by snapshot ID and
 * snapshot IDs behind named refs (branches and tags).
 */
final class IcebergSnapshotResolver {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSnapshotResolver.class);

    private IcebergSnapshotResolver() {}

    /** Returns the commit timestamp of the given snapshot, or empty if absent. */
    static Optional<Instant> resolveTimestamp(Catalog catalog, String schemaName,
                                              String tableName, long snapshotId) {
        Table table = catalog.loadTable(TableIdentifier.of(schemaName, tableName));
        Snapshot snapshot = table.snapshot(snapshotId);
        if (snapshot == null) {
            return Optional.empty();
        }
        return Optional.of(Instant.ofEpochMilli(snapshot.timestampMillis()));
    }

    /** Returns the snapshot ID behind a reference of the requested kind, or empty if absent. */
    static OptionalLong resolveRefSnapshotId(Catalog catalog, SchemaTableName tableName,
                                             String refType, String refName) {
        TableIdentifier tableId = TableIdentifier.of(tableName.schema(), tableName.table());
        Table table = catalog.loadTable(tableId);
        return resolveRefSnapshotId(table, refType, refName);
    }

    static OptionalLong resolveRefSnapshotId(Table table, String refType, String refName) {
        SnapshotRef ref = table.refs().get(refName);
        if (ref == null || !matchesKind(ref, refType)) {
            return OptionalLong.empty();
        }
        LOG.info("event=ref_snapshot_resolved table={} refType={} refName={} snapshotId={}",
                table.name(), refType, refName, ref.snapshotId());
        return OptionalLong.of(ref.snapshotId());
    }

    private static boolean matchesKind(SnapshotRef ref, String refType) {
        return ("BRANCH".equals(refType) && ref.isBranch())
                || ("TAG".equals(refType) && ref.isTag());
    }
}
