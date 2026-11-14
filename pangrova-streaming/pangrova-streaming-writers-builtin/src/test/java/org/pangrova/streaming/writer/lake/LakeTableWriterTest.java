package org.pangrova.streaming.writer.lake;

import org.pangrova.connector.api.enums.WriteOperation;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.write.DataFileSink;
import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.enums.WriteMode;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.connector.api.write.WriteResult;
import org.pangrova.connector.api.enums.TableFormat;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies LakeTableWriter delegates to DataFileSink and returns
 * actual data file counts instead of hardcoded values.
 */
class LakeTableWriterTest {

    @Test
    void writeDelegatesToDataFileSinkAndReturnsActualFileCount() {
        RecordingDataFileSink sink = new RecordingDataFileSink(3);
        try (LakeTableWriter writer = new LakeTableWriter(sink, "id", null)) {
            List<WriteRecord> records = List.of(
                    insertRecord(1, "alice"),
                    insertRecord(2, "bob"),
                    insertRecord(3, "charlie"));

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
    void zeroFilesReturnedWhenSinkWritesNothing() {
        RecordingDataFileSink sink = new RecordingDataFileSink(0);
        try (LakeTableWriter writer = new LakeTableWriter(sink, "id", null)) {
            WriteResult result = writer.write("db.empty", List.of(insertRecord(1, "a")),
                    WriteMode.APPEND, WriteContext.selfManaged());

            assertThat(result.dataFilesWritten()).isEqualTo(0);
        }
    }

    @Test
    void emptyInsertsSkipDataFileWrite() {
        RecordingDataFileSink sink = new RecordingDataFileSink(5);
        try (LakeTableWriter writer = new LakeTableWriter(sink, "id", null)) {
            // Single delete record → classified as equality delete, no inserts
            WriteResult result = writer.write("db.table1",
                    List.of(deleteRecord(1)),
                    WriteMode.APPEND, WriteContext.selfManaged());

            assertThat(result.dataFilesWritten()).isEqualTo(0);
            assertThat(result.equalityDeleteFiles()).isEqualTo(1);
            // Delete records are written to the sink for materialisation
            assertThat(sink.receivedTableName).isEqualTo("db.table1");
        }
    }

    @Test
    void formatReturnsIceberg() {
        RecordingDataFileSink sink = new RecordingDataFileSink(0);
        try (LakeTableWriter writer = new LakeTableWriter(sink, "id", null)) {
            assertThat(writer.format()).isEqualTo(TableFormat.ICEBERG);
        }
    }

    @Test
    void closeClosesDataFileSink() {
        RecordingDataFileSink sink = new RecordingDataFileSink(0);
        LakeTableWriter writer = new LakeTableWriter(sink, "id", null);
        writer.close();

        assertThat(sink.closed).isTrue();
    }

    @Test
    void nullDataFileSinkRejected() {
        assertThatThrownBy(() -> new LakeTableWriter(null, "id", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("dataFileSink");
    }

    @Test
    void duplicateKeysStillDelegateInsertToSink() {
        RecordingDataFileSink sink = new RecordingDataFileSink(2);
        try (LakeTableWriter writer = new LakeTableWriter(sink, "id", null)) {
            // Two records with same key → 1 position delete + 1 insert
            List<WriteRecord> records = List.of(
                    insertRecord(1, "v1"),
                    insertRecord(1, "v2"));

            WriteResult result = writer.write("db.table1", records,
                    WriteMode.APPEND, WriteContext.selfManaged());

            assertThat(result.dataFilesWritten()).isEqualTo(2);
            assertThat(result.positionDeleteFiles()).isEqualTo(1);
            assertThat(sink.receivedRecords).hasSize(1);
        }
    }

    // --- test helpers ---

    private static WriteRecord insertRecord(int id, String name) {
        return new WriteRecord(
                TypedRow.fromRawMap(Map.of("id", id, "name", name)),
                WriteOperation.INSERT, Instant.now());
    }

    private static WriteRecord deleteRecord(int id) {
        return new WriteRecord(
                TypedRow.fromRawMap(Map.of("id", id)),
                WriteOperation.DELETE, Instant.now());
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
