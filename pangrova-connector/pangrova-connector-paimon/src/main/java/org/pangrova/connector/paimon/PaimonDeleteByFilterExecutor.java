package org.pangrova.connector.paimon;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.metadata.DeleteFilter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements {@code deleteByFilter} on primary-key Paimon tables by reading
 * all rows, filtering out matches, and overwriting the affected partitions
 * with the surviving rows.
 */
final class PaimonDeleteByFilterExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDeleteByFilterExecutor.class);

    private PaimonDeleteByFilterExecutor() {}

    /** Executes a filter-based delete against the given fresh table. */
    static void execute(Table freshTable, DeleteFilter filter,
                        String schemaName, String tableName) {
        List<DataField> fields = freshTable.rowType().getFields();
        List<String> partitionKeys = freshTable.partitionKeys();

        // Read all rows using Paimon's native reader (handles merge-on-read correctly)
        List<InternalRow> allRows = readAllRows(freshTable, fields);

        // For non-partitioned tables, do a single full overwrite
        if (partitionKeys.isEmpty()) {
            List<InternalRow> remaining = allRows.stream()
                    .filter(row -> !PaimonDeleteFilterMatcher.matches(row, fields, filter))
                    .toList();
            overwriteWithPaimonRows(freshTable, Map.of(), remaining);
            LOG.info("event=paimon_delete_by_filter_full schema={} table={} deleted={}",
                    schemaName, tableName, allRows.size() - remaining.size());
            return;
        }

        int totalDeleted = overwriteAffectedPartitions(freshTable, allRows, fields, partitionKeys, filter);
        LOG.info("event=paimon_delete_by_filter schema={} table={} deleted={}",
                schemaName, tableName, totalDeleted);
    }

    /**
     * Per-partition overwrite: for each affected partition, overwrite with
     * the surviving rows. When ALL rows in a partition are deleted, Paimon
     * skips empty commits (no messages = no-op). To handle this, we overwrite
     * with the deleted rows marked as RowKind.DELETE — the overwrite clears
     * old data, and the DELETE markers are skipped by merge-on-read.
     */
    private static int overwriteAffectedPartitions(Table freshTable, List<InternalRow> allRows,
                                                   List<DataField> fields, List<String> partitionKeys,
                                                   DeleteFilter filter) {
        var allByPartition = groupInternalRowsByPartition(allRows, fields, partitionKeys);
        int totalDeleted = 0;
        for (var entry : allByPartition.entrySet()) {
            List<InternalRow> allInPartition = entry.getValue();
            List<InternalRow> remaining = allInPartition.stream()
                    .filter(row -> !PaimonDeleteFilterMatcher.matches(row, fields, filter))
                    .toList();
            if (remaining.size() == allInPartition.size()) {
                continue;
            }
            totalDeleted += allInPartition.size() - remaining.size();
            overwritePartition(freshTable, fields, entry.getKey(), allInPartition, remaining);
        }
        return totalDeleted;
    }

    private static void overwritePartition(Table freshTable, List<DataField> fields,
                                           Map<String, String> partitionValues,
                                           List<InternalRow> allInPartition,
                                           List<InternalRow> remaining) {
        if (!remaining.isEmpty()) {
            overwriteWithPaimonRows(freshTable, partitionValues, remaining);
            return;
        }
        // All rows deleted — overwrite with DELETE-marked rows so the
        // commit actually fires (empty messages are a no-op in Paimon).
        List<InternalRow> deleteMarked = new ArrayList<>();
        for (InternalRow row : allInPartition) {
            GenericRow copy = PaimonInternalRows.copy(row, fields);
            copy.setRowKind(RowKind.DELETE);
            deleteMarked.add(copy);
        }
        overwriteWithPaimonRows(freshTable, partitionValues, deleteMarked);
    }

    private static List<InternalRow> readAllRows(Table freshTable, List<DataField> fields) {
        List<InternalRow> allRows = new ArrayList<>();
        ReadBuilder readBuilder = freshTable.newReadBuilder();
        TableScan scan = readBuilder.newScan();
        TableRead tableRead = readBuilder.newRead();
        try {
            for (Split split : scan.plan().splits()) {
                readSplitInto(tableRead, split, fields, allRows);
            }
        } catch (IOException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Failed to read rows for deleteByFilter", e);
        }
        return allRows;
    }

    private static void readSplitInto(TableRead tableRead, Split split,
                                      List<DataField> fields,
                                      List<InternalRow> allRows) throws IOException {
        try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    allRows.add(PaimonInternalRows.copy(row, fields));
                }
                batch.releaseBatch();
            }
        }
    }

    private static Map<Map<String, String>, List<InternalRow>> groupInternalRowsByPartition(
            List<InternalRow> rows, List<DataField> fields, List<String> partitionKeys) {
        var grouped = new LinkedHashMap<Map<String, String>, List<InternalRow>>();
        if (partitionKeys.isEmpty()) {
            grouped.put(Map.of(), new ArrayList<>(rows));
            return grouped;
        }
        for (InternalRow row : rows) {
            var partKey = new LinkedHashMap<String, String>();
            for (String pk : partitionKeys) {
                for (int i = 0; i < fields.size(); i++) {
                    if (fields.get(i).name().equals(pk)) {
                        Object val = PaimonInternalRows.getFieldValue(row, i, fields.get(i));
                        partKey.put(pk, val != null ? val.toString() : "__null__");
                        break;
                    }
                }
            }
            grouped.computeIfAbsent(partKey, k -> new ArrayList<>()).add(row);
        }
        return grouped;
    }

    private static void overwriteWithPaimonRows(
            Table table, Map<String, String> partitionValues, List<InternalRow> rows) {
        BatchWriteBuilder builder = table.newBatchWriteBuilder().withOverwrite(partitionValues);
        String tempDir = System.getProperty("java.io.tmpdir") + "/paimon-io";
        try (org.apache.paimon.disk.IOManager ioManager =
                     new org.apache.paimon.disk.IOManagerImpl(tempDir);
             BatchTableWrite write = builder.newWrite()) {
            write.withIOManager(ioManager);
            for (InternalRow row : rows) {
                write.write(row);
            }
            List<CommitMessage> messages = write.prepareCommit();
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(messages);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to overwrite partition during deleteByFilter", e);
        }
    }
}
