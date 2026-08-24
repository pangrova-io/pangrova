package org.pangrova.connector.iceberg.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Iceberg manifest files via {@code table$manifests}.
 *
 * <p>Lists all manifests from the current snapshot with path, length,
 * partition spec ID, added snapshot ID, and added data files count.</p>
 */
public final class IcebergManifestsSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergManifestsSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "path", "length", "partition_spec_id", "added_snapshot_id", "added_data_files_count");

    private final Table table;

    public IcebergManifestsSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }

    @Override
    public List<TypedRow> rows() {
        Snapshot current = table.currentSnapshot();
        if (current == null) {
            return List.of();
        }
        List<TypedRow> rows = new ArrayList<>();
        try {
            for (ManifestFile manifest : current.allManifests(table.io())) {
                TypedRow row = TypedRow.builder()
                        .putString("path", manifest.path())
                        .putLong("length", manifest.length())
                        .putInt("partition_spec_id", manifest.partitionSpecId())
                        .putLong("added_snapshot_id", manifest.snapshotId())
                        .putRaw("added_data_files_count",
                                manifest.addedFilesCount() != null ? manifest.addedFilesCount() : null)
                        .build();
                rows.add(row);
            }
        } catch (Exception e) {
            LOG.warn("event=manifests_system_table_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }
}
