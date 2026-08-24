package org.pangrova.connector.iceberg.system;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;

/**
 * Exposes Iceberg snapshot references (branches and tags) via {@code table$refs}.
 *
 * <p>Each row represents a named reference with its type (branch or tag),
 * snapshot ID, and optional max reference age.</p>
 */
public final class IcebergRefsSystemTable implements ConnectorSystemTable {

    private static final List<String> COLUMNS = List.of(
            "name", "type", "snapshot_id", "max_ref_age_in_ms");

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
                    .putRaw("max_ref_age_in_ms",
                            ref.maxRefAgeMs() != null ? ref.maxRefAgeMs() : null)
                    .build();
            rows.add(row);
        }
        return List.copyOf(rows);
    }
}
