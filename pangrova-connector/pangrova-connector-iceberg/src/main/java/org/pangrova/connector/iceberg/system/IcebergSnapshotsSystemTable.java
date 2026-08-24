package org.pangrova.connector.iceberg.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Exposes Iceberg table snapshot history via {@code table$snapshots}.
 *
 * <p>Each row represents one snapshot with its ID, parent, timestamp,
 * operation type, and summary map serialized as a string.</p>
 */
public final class IcebergSnapshotsSystemTable implements ConnectorSystemTable {

    private static final List<String> COLUMNS = List.of(
            "snapshot_id", "parent_id", "timestamp_ms", "operation", "summary");

    private final Table table;

    public IcebergSnapshotsSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }

    @Override
    public List<TypedRow> rows() {
        List<TypedRow> rows = new ArrayList<>();
        for (Snapshot snapshot : table.snapshots()) {
            TypedRow row = TypedRow.builder()
                    .putLong("snapshot_id", snapshot.snapshotId())
                    .putRaw("parent_id", snapshot.parentId() != null ? snapshot.parentId() : null)
                    .putLong("timestamp_ms", snapshot.timestampMillis())
                    .putString("operation", snapshot.operation())
                    .putString("summary", snapshot.summary() != null ? snapshot.summary().toString() : null)
                    .build();
            rows.add(row);
        }
        return List.copyOf(rows);
    }
}
