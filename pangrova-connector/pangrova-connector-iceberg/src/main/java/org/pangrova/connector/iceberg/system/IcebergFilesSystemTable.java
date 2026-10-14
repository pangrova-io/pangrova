package org.pangrova.connector.iceberg.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Iceberg data files via {@code table$files}.
 *
 * <p>Scans the current snapshot and returns one row per data file
 * with content type, path, format, record count, and size.</p>
 */
public final class IcebergFilesSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "content", "file_path", "file_format", "record_count", "file_size_in_bytes");

    private final Table table;

    public IcebergFilesSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return List.copyOf(COLUMNS);
    }

    @Override
    public List<TypedRow> rows() {
        if (table.currentSnapshot() == null) {
            return List.of();
        }
        List<TypedRow> rows = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                TypedRow row = TypedRow.builder()
                        .putString("content", file.content().name())
                        .putString("file_path", file.path().toString())
                        .putString("file_format", file.format().name())
                        .putLong("record_count", file.recordCount())
                        .putLong("file_size_in_bytes", file.fileSizeInBytes())
                        .build();
                rows.add(row);
            }
        } catch (java.io.IOException e) {
            LOG.warn("event=files_system_table_scan_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }
}
