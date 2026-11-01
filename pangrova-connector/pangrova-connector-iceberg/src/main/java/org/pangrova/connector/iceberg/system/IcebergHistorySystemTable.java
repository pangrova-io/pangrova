package org.pangrova.connector.iceberg.system;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Exposes Iceberg table history via {@code table$history}.
 *
 * <p>Each row represents a history entry showing when a snapshot
 * became current, its ID, parent ID, and whether it is an ancestor
 * of the current snapshot.</p>
 */
public final class IcebergHistorySystemTable implements ConnectorSystemTable {

    private static final List<String> COLUMNS = List.of(
            "made_current_at", "snapshot_id", "parent_id", "is_current_ancestor");

    private final Table table;

    public IcebergHistorySystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }

    @Override
    public List<TypedRow> rows() {
        Set<Long> ancestorIds = collectAncestorIds();
        List<TypedRow> rows = new ArrayList<>();
        for (HistoryEntry entry : table.history()) {
            Snapshot snapshot = table.snapshot(entry.snapshotId());
            Long parentId = snapshot != null ? snapshot.parentId() : null;
            TypedRow row = TypedRow.builder()
                    .putLong("made_current_at", entry.timestampMillis())
                    .putLong("snapshot_id", entry.snapshotId())
                    .putRaw("parent_id", parentId)
                    .putRaw("is_current_ancestor", ancestorIds.contains(entry.snapshotId()))
                    .build();
            rows.add(row);
        }
        return List.copyOf(rows);
    }

    private Set<Long> collectAncestorIds() {
        Set<Long> ancestors = new HashSet<>();
        Snapshot current = table.currentSnapshot();
        while (current != null) {
            ancestors.add(current.snapshotId());
            Long parentId = current.parentId();
            current = parentId != null ? table.snapshot(parentId) : null;
        }
        return ancestors;
    }
}
