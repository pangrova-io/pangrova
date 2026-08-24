package org.pangrova.connector.iceberg.system;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Iceberg partition statistics via {@code table$partitions}.
 *
 * <p>Groups data files by partition value and returns aggregate
 * record count and file count per partition.</p>
 */
public final class IcebergPartitionsSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergPartitionsSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "partition", "record_count", "file_count");

    private final Table table;

    public IcebergPartitionsSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }

    @Override
    public List<TypedRow> rows() {
        if (table.currentSnapshot() == null) {
            return List.of();
        }
        Map<String, long[]> partitionStats = new LinkedHashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                String partitionKey = partitionToString(file.partition());
                partitionStats.merge(partitionKey,
                        new long[]{file.recordCount(), 1},
                        (a, b) -> {
                            a[0] += b[0];
                            a[1] += b[1];
                            return a;
                        });
            }
        } catch (java.io.IOException e) {
            LOG.warn("event=partitions_system_table_scan_failed error={}", e.getMessage());
        }
        List<TypedRow> rows = new ArrayList<>();
        for (Map.Entry<String, long[]> entry : partitionStats.entrySet()) {
            long[] stats = entry.getValue();
            TypedRow row = TypedRow.builder()
                    .putString("partition", entry.getKey())
                    .putLong("record_count", stats[0])
                    .putLong("file_count", stats[1])
                    .build();
            rows.add(row);
        }
        return List.copyOf(rows);
    }

    private static String partitionToString(StructLike partition) {
        int size = partition.size();
        if (size == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Object val = partition.get(i, Object.class);
            sb.append(val != null ? val.toString() : "null");
        }
        sb.append("}");
        return sb.toString();
    }
}
