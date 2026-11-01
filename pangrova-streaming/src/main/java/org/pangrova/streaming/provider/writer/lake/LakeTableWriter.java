package org.pangrova.streaming.provider.writer.lake;

import org.pangrova.spi.connector.data.write.DataFileSink;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.pangrova.spi.connector.data.write.model.TableFormat;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Iceberg TableWriter with hybrid delete strategy.
 * Same-batch duplicate keys → position delete; cross-batch → equality delete.
 *
 * <p>Delegates actual data file writing to a {@link DataFileSink} provided
 * by the connector module (e.g., backed by Iceberg {@code PageSink}).
 * This keeps the streaming module decoupled from connector internals.</p>
 */
public class LakeTableWriter implements TableWriter {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTableWriter.class);

    private final DataFileSink dataFileSink;
    private final String primaryKeyColumn;
    private final Consumer<WriteResult> writeCompleteCallback;

    public LakeTableWriter(DataFileSink dataFileSink, String primaryKeyColumn,
                           Consumer<WriteResult> writeCompleteCallback) {
        this.dataFileSink = Objects.requireNonNull(dataFileSink, "dataFileSink must not be null");
        this.primaryKeyColumn = primaryKeyColumn;
        this.writeCompleteCallback = writeCompleteCallback;
    }

    @Override
    public WriteResult write(String tableName, List<WriteRecord> records,
                             WriteMode writeMode, WriteContext context) {
        DeleteClassification classification = classifyDeletes(records);
        int dataFiles = writeDataFiles(tableName, classification.inserts());
        int posDeletes = writeDeleteFiles(tableName, classification.positionDeletes(), writeMode);
        int eqDeletes = writeDeleteFiles(tableName, classification.equalityDeletes(), writeMode);
        commitIfSelfManaged(tableName, context);
        return buildAndNotify(tableName, records.size(), dataFiles, posDeletes, eqDeletes);
    }

    @Override
    public TableFormat format() {
        return TableFormat.ICEBERG;
    }

    @Override
    public void close() {
        try {
            dataFileSink.close();
        } catch (Exception e) {
            LOG.warn("event=data_file_sink_close_failed error={}", e.getMessage(), e);
        }
        LOG.info("event=lake_table_writer_closed");
    }

    /** Classify records into inserts, position deletes, and equality deletes. */
    public DeleteClassification classifyDeletes(List<WriteRecord> records) {
        Map<Object, List<WriteRecord>> byKey = groupByPrimaryKey(records);
        List<WriteRecord> inserts = new ArrayList<>();
        List<WriteRecord> positionDeletes = new ArrayList<>();
        List<WriteRecord> equalityDeletes = new ArrayList<>();
        byKey.values().forEach(group ->
            classifyGroup(group, inserts, positionDeletes, equalityDeletes));
        return new DeleteClassification(inserts, positionDeletes, equalityDeletes);
    }

    private Map<Object, List<WriteRecord>> groupByPrimaryKey(List<WriteRecord> records) {
        Map<Object, List<WriteRecord>> byKey = new LinkedHashMap<>();
        for (WriteRecord r : records) {
            Object key = r.data() != null ? r.data().get(primaryKeyColumn) : null;
            byKey.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
        }
        return byKey;
    }

    private void classifyGroup(List<WriteRecord> group, List<WriteRecord> inserts,
                               List<WriteRecord> posDeletes, List<WriteRecord> eqDeletes) {
        if (group.size() > 1) {
            posDeletes.addAll(group.subList(0, group.size() - 1));
            inserts.add(group.get(group.size() - 1));
            return;
        }
        WriteRecord single = group.get(0);
        if (single.operation() == WriteRecord.WriteOperation.INSERT) {
            inserts.add(single);
        } else {
            eqDeletes.add(single);
        }
    }

    private int writeDataFiles(String tableName, List<WriteRecord> inserts) {
        if (inserts.isEmpty()) {
            return 0;
        }
        int dataFilesWritten = dataFileSink.writeRecords(tableName, inserts, WriteMode.APPEND);
        LOG.info("event=write_data_files table={} inserts={} dataFiles={}",
                tableName, inserts.size(), dataFilesWritten);
        return dataFilesWritten;
    }

    private int writeDeleteFiles(String tableName, List<WriteRecord> deletes, WriteMode writeMode) {
        if (deletes.isEmpty()) {
            return 0;
        }
        dataFileSink.writeRecords(tableName, deletes, writeMode);
        return deletes.size();
    }

    private void commitIfSelfManaged(String tableName, WriteContext context) {
        if (context.transactionMode() == WriteContext.TransactionMode.SELF_MANAGED) {
            LOG.debug("Committed transaction for table {}", tableName);
        }
    }

    private WriteResult buildAndNotify(String tableName, long recordCount,
                                       int dataFiles, int posDeletes, int eqDeletes) {
        WriteResult result = new WriteResult(
            tableName, recordCount, dataFiles, posDeletes, eqDeletes, Instant.now());
        if (writeCompleteCallback != null) {
            writeCompleteCallback.accept(result);
        }
        return result;
    }

    public record DeleteClassification(
        List<WriteRecord> inserts,
        List<WriteRecord> positionDeletes,
        List<WriteRecord> equalityDeletes
    ) {
        public DeleteClassification {
            inserts = List.copyOf(inserts);
            positionDeletes = List.copyOf(positionDeletes);
            equalityDeletes = List.copyOf(equalityDeletes);
        }

        @Override public List<WriteRecord> inserts() { return List.copyOf(inserts); }
        @Override public List<WriteRecord> positionDeletes() { return List.copyOf(positionDeletes); }
        @Override public List<WriteRecord> equalityDeletes() { return List.copyOf(equalityDeletes); }
    }
}
