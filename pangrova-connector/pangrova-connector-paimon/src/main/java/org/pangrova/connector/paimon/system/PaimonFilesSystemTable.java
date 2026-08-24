package org.pangrova.connector.paimon.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Paimon data files via {@code table$files}.
 *
 * <p>Each row represents a data file with its partition, bucket,
 * file path, file format, file size, and row count.</p>
 */
public final class PaimonFilesSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonFilesSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "partition", "bucket", "file_path", "file_format", "file_size", "row_count");

    private final Table table;

    public PaimonFilesSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }

    @Override
    public List<TypedRow> rows() {
        List<TypedRow> rows = new ArrayList<>();
        try {
            List<Split> splits = table.newReadBuilder().newScan().plan().splits();
            for (Split split : splits) {
                if (!(split instanceof DataSplit dataSplit)) {
                    continue;
                }
                String partition = dataSplit.partition().toString();
                int bucket = dataSplit.bucket();
                for (DataFileMeta file : dataSplit.dataFiles()) {
                    TypedRow row = TypedRow.builder()
                            .putString("partition", partition)
                            .putInt("bucket", bucket)
                            .putString("file_path", file.fileName())
                            .putString("file_format", file.fileFormat())
                            .putLong("file_size", file.fileSize())
                            .putLong("row_count", file.rowCount())
                            .build();
                    rows.add(row);
                }
            }
        } catch (Exception e) {
            LOG.warn("event=files_system_table_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }
}
