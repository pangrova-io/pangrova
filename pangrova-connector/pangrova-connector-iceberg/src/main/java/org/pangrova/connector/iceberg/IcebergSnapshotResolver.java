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

    /** Returns the snapshot ID behind the named ref, or empty if absent. */
    static OptionalLong resolveRefSnapshotId(Catalog catalog, SchemaTableName tableName,
                                             String refType, String refName) {
        TableIdentifier tableId = TableIdentifier.of(tableName.schema(), tableName.table());
        Table table = catalog.loadTable(tableId);
        SnapshotRef ref = table.refs().get(refName);
        if (ref == null) {
            return OptionalLong.empty();
        }
        LOG.info("event=ref_snapshot_resolved table={} refType={} refName={} snapshotId={}",
                tableName, refType, refName, ref.snapshotId());
        return OptionalLong.of(ref.snapshotId());
    }
}
