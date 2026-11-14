package org.pangrova.connector.iceberg.system;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.ConnectorSystemTable;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;

/**
 * Exposes Iceberg snapshot references (branches and tags) via {@code table$refs}.
 *
 * <p>Each row represents a named reference with its type (branch or tag),
 * snapshot ID and nullable reference/snapshot retention settings.</p>
 */
public final class IcebergRefsSystemTable implements ConnectorSystemTable {

    private static final List<String> COLUMNS = List.of(
            "name", "type", "snapshot_id", "max_reference_age_in_ms",
            "min_snapshots_to_keep", "max_snapshot_age_in_ms");

    private final Table table;

    public IcebergRefsSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }

    @Override
    public List<TypedRow> rows() {
        Map<String, SnapshotRef> refs = table.refs();
        if (refs == null || refs.isEmpty()) {
            return List.of();
        }
        List<TypedRow> rows = new ArrayList<>();
        for (Map.Entry<String, SnapshotRef> entry : refs.entrySet()) {
            SnapshotRef ref = entry.getValue();
            TypedRow row = TypedRow.builder()
                    .putString("name", entry.getKey())
                    .putString("type", ref.isBranch() ? "BRANCH" : "TAG")
                    .putLong("snapshot_id", ref.snapshotId())
                    .putRaw("max_reference_age_in_ms", ref.maxRefAgeMs())
                    .putRaw("min_snapshots_to_keep", ref.minSnapshotsToKeep())
                    .putRaw("max_snapshot_age_in_ms", ref.maxSnapshotAgeMs())
                    .build();
            rows.add(row);
        }
        return List.copyOf(rows);
    }
}
