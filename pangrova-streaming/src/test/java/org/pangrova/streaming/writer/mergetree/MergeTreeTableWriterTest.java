package org.pangrova.streaming.writer.mergetree;

import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.spi.write.DataFileSink;
import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.write.WriteMode;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.connector.api.write.WriteResult;
import org.pangrova.connector.api.write.TableFormat;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies MergeTreeTableWriter delegates to DataFileSink and returns
 * actual data file counts instead of hardcoded values.
 */
class MergeTreeTableWriterTest {

    @Test
    void writeDelegatesToDataFileSinkAndReturnsActualFileCount() {
        RecordingDataFileSink sink = new RecordingDataFileSink(3);
        try (MergeTreeTableWriter writer = new MergeTreeTableWriter(sink, null)) {
            List<WriteRecord> records = List.of(
                    insertRecord("col1", "value1"),
                    insertRecord("col1", "value2"),
                    insertRecord("col1", "value3"));

            WriteResult result = writer.write("db.table1", records,
                    WriteMode.APPEND, WriteContext.selfManaged());

            assertThat(result.dataFilesWritten()).isEqualTo(3);
            assertThat(result.recordCount()).isEqualTo(3);
            assertThat(result.tableName()).isEqualTo("db.table1");
            assertThat(sink.receivedTableName).isEqualTo("db.table1");
            assertThat(sink.receivedRecords).hasSize(3);
            assertThat(sink.receivedWriteMode).isEqualTo(WriteMode.APPEND);
        }
    }

    @Test
    void writeWithUpsertModeDelegatesToSink() {
        RecordingDataFileSink sink = new RecordingDataFileSink(2);
        try (MergeTreeTableWriter writer = new MergeTreeTableWriter(sink, null)) {
            List<WriteRecord> records = List.of(
                    updateRecord("id", 1),
                    deleteRecord("id", 2));

            WriteResult result = writer.write("db.table2", records,
                    WriteMode.UPSERT, WriteContext.selfManaged());

            assertThat(result.dataFilesWritten()).isEqualTo(2);
            assertThat(result.recordCount()).isEqualTo(2);
            assertThat(sink.receivedWriteMode).isEqualTo(WriteMode.UPSERT);
        }
    }

    @Test
    void zeroFilesReturnedWhenSinkWritesNothing() {
        RecordingDataFileSink sink = new RecordingDataFileSink(0);
        try (MergeTreeTableWriter writer = new MergeTreeTableWriter(sink, null)) {
            WriteResult result = writer.write("db.empty", List.of(insertRecord("a", "b")),
                    WriteMode.APPEND, WriteContext.selfManaged());

            assertThat(result.dataFilesWritten()).isEqualTo(0);
        }
    }

    @Test
    void formatReturnsPaimon() {
        RecordingDataFileSink sink = new RecordingDataFileSink(0);
        try (MergeTreeTableWriter writer = new MergeTreeTableWriter(sink, null)) {
            assertThat(writer.format()).isEqualTo(TableFormat.PAIMON);
        }
    }

    @Test
    void closeClosesDataFileSink() {
        RecordingDataFileSink sink = new RecordingDataFileSink(0);
        MergeTreeTableWriter writer = new MergeTreeTableWriter(sink, null);
        writer.close();

        assertThat(sink.closed).isTrue();
    }

    @Test
    void nullDataFileSinkRejected() {
        assertThatThrownBy(() -> new MergeTreeTableWriter(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("dataFileSink");
    }

    @Test
    void mapToRowKindCoversAllOperations() {
        assertThat(MergeTreeTableWriter.mapToRowKind(WriteRecord.WriteOperation.INSERT))
                .isEqualTo(MergeTreeTableWriter.RowKind.INSERT);
        assertThat(MergeTreeTableWriter.mapToRowKind(WriteRecord.WriteOperation.UPDATE))
                .isEqualTo(MergeTreeTableWriter.RowKind.UPDATE_AFTER);
        assertThat(MergeTreeTableWriter.mapToRowKind(WriteRecord.WriteOperation.DELETE))
                .isEqualTo(MergeTreeTableWriter.RowKind.DELETE);
    }

    // --- test helpers ---

    private static WriteRecord insertRecord(String col, Object val) {
        return new WriteRecord(
                TypedRow.fromRawMap(Map.of(col, val)),
                WriteRecord.WriteOperation.INSERT, Instant.now());
    }

    private static WriteRecord updateRecord(String col, Object val) {
        return new WriteRecord(
                TypedRow.fromRawMap(Map.of(col, val)),
                WriteRecord.WriteOperation.UPDATE, Instant.now());
    }

    private static WriteRecord deleteRecord(String col, Object val) {
        return new WriteRecord(
                TypedRow.fromRawMap(Map.of(col, val)),
                WriteRecord.WriteOperation.DELETE, Instant.now());
    }

    /**
     * Simple recording implementation of DataFileSink for testing.
     * Returns a configurable file count and records all received arguments.
     */
    static final class RecordingDataFileSink implements DataFileSink {
        private final int filesToReturn;
        String receivedTableName;
        List<WriteRecord> receivedRecords;
        WriteMode receivedWriteMode;
        boolean closed;

        RecordingDataFileSink(int filesToReturn) {
            this.filesToReturn = filesToReturn;
        }

        @Override
        public int writeRecords(String tableName, List<WriteRecord> records, WriteMode writeMode) {
            this.receivedTableName = tableName;
            this.receivedRecords = new ArrayList<>(records);
            this.receivedWriteMode = writeMode;
            return filesToReturn;
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}
