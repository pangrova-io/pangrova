package org.pangrova.connector.paimon.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Paimon snapshot metadata via {@code table$snapshots}.
 *
 * <p>Each row represents a committed snapshot with its ID, schema ID,
 * commit kind, commit timestamp, and total record count.</p>
 */
public final class PaimonSnapshotsSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSnapshotsSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "snapshot_id", "schema_id", "commit_kind", "commit_time", "total_record_count");

    private final Table table;

    public PaimonSnapshotsSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return List.copyOf(COLUMNS);
    }

    @Override
    public List<TypedRow> rows() {
        if (!(table instanceof FileStoreTable fst)) {
            return List.of();
        }
        SnapshotManager snapshotManager = fst.snapshotManager();
        Long earliest = snapshotManager.earliestSnapshotId();
        Long latest = snapshotManager.latestSnapshotId();
        if (earliest == null || latest == null) {
            return List.of();
        }
        List<TypedRow> rows = new ArrayList<>();
        for (long id = earliest; id <= latest; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot == null) {
                continue;
            }
            TypedRow row = TypedRow.builder()
                    .putLong("snapshot_id", snapshot.id())
                    .putLong("schema_id", snapshot.schemaId())
                    .putString("commit_kind", snapshot.commitKind().name())
                    .putLong("commit_time", snapshot.timeMillis())
                    .putLong("total_record_count", snapshot.totalRecordCount())
                    .build();
            rows.add(row);
        }
        return List.copyOf(rows);
    }
}
