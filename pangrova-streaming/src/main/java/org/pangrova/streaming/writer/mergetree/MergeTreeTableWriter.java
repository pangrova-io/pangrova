package org.pangrova.streaming.writer.mergetree;

import org.pangrova.connector.spi.write.DataFileSink;
import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.write.WriteMode;
import org.pangrova.connector.api.write.WriteResult;
import org.pangrova.connector.api.write.TableFormat;
import org.pangrova.connector.spi.write.TableWriter;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Paimon TableWriter with native changelog support.
 * Maps WriteOperation → RowKind for Paimon's changelog semantics.
 *
 * <p>Delegates actual data file writing to a {@link DataFileSink} provided
 * by the connector module (e.g., backed by Paimon {@code TableWrite} API).
 * This keeps the streaming module decoupled from connector internals.</p>
 */
public class MergeTreeTableWriter implements TableWriter {

    private static final Logger LOG = LoggerFactory.getLogger(MergeTreeTableWriter.class);

    private final DataFileSink dataFileSink;
    private final MaintenanceScheduler scheduler;

    public MergeTreeTableWriter(DataFileSink dataFileSink, MaintenanceScheduler scheduler) {
        this.dataFileSink = Objects.requireNonNull(dataFileSink, "dataFileSink must not be null");
        this.scheduler = scheduler;
    }

    @Override
    public WriteResult write(String tableName, List<WriteRecord> records,
                             WriteMode writeMode, WriteContext context) {
        for (WriteRecord record : records) {
            RowKind rowKind = mapToRowKind(record.operation());
            LOG.debug("event=write_record rowKind={} table={}", rowKind, tableName);
        }
        int dataFilesWritten = dataFileSink.writeRecords(tableName, records, writeMode);
        LOG.info("event=write_complete table={} records={} dataFiles={}",
                tableName, records.size(), dataFilesWritten);
        WriteResult result = new WriteResult(
                tableName, records.size(), dataFilesWritten, 0, 0, Instant.now());
        notifyScheduler(tableName, result);
        return result;
    }

    @Override
    public TableFormat format() {
        return TableFormat.PAIMON;
    }

    @Override
    public void close() {
        try {
            dataFileSink.close();
        } catch (Exception e) {
            LOG.warn("event=data_file_sink_close_failed error={}", e.getMessage(), e);
        }
        LOG.info("event=merge_tree_table_writer_closed");
    }

    /** Map WriteOperation to Paimon RowKind. */
    public static RowKind mapToRowKind(WriteRecord.WriteOperation op) {
        return switch (op) {
            case INSERT -> RowKind.INSERT;
            case UPDATE -> RowKind.UPDATE_AFTER;
            case DELETE -> RowKind.DELETE;
        };
    }

    private void notifyScheduler(String tableName, WriteResult result) {
        if (scheduler != null) {
            scheduler.onWriteComplete(tableName, result);
        }
    }

    /** Paimon RowKind representation. */
    public enum RowKind {
        INSERT,
        UPDATE_BEFORE,
        UPDATE_AFTER,
        DELETE
    }
}
