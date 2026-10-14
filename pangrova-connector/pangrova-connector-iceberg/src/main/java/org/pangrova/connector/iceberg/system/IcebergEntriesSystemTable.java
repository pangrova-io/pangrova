package org.pangrova.connector.iceberg.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Iceberg data file entries via {@code table$entries}.
 *
 * <p>Each row represents a data file entry with the snapshot ID,
 * sequence number, file path, and data sequence number.</p>
 */
public final class IcebergEntriesSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergEntriesSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "snapshot_id", "sequence_number", "file_path", "data_sequence_number");

    private final Table table;

    public IcebergEntriesSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return List.copyOf(COLUMNS);
    }

    @Override
    public List<TypedRow> rows() {
        Snapshot current = table.currentSnapshot();
        if (current == null) {
            return List.of();
        }
        long snapshotId = current.snapshotId();
        long sequenceNumber = current.sequenceNumber();
        List<TypedRow> rows = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                TypedRow row = TypedRow.builder()
                        .putLong("snapshot_id", snapshotId)
                        .putLong("sequence_number", sequenceNumber)
                        .putString("file_path", file.path().toString())
                        .putRaw("data_sequence_number", file.dataSequenceNumber())
                        .build();
                rows.add(row);
            }
        } catch (java.io.IOException e) {
            LOG.warn("event=entries_system_table_scan_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }
}
