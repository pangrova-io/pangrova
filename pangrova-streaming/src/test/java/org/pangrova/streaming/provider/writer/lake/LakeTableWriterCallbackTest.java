package org.pangrova.streaming.provider.writer.lake;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.data.write.DataFileSink;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies LakeTableWriter callback decoupling (Task 12.1).
 * Callback is invoked on write complete; null callback is safe.
 */
class LakeTableWriterCallbackTest {

    @Test
    void callbackInvokedOnWriteComplete() {
        List<WriteResult> captured = new ArrayList<>();
        RecordingDataFileSink sink = new RecordingDataFileSink(1);
        try (LakeTableWriter writer = new LakeTableWriter(sink, "id", captured::add)) {
            WriteRecord record = new WriteRecord(
                    TypedRow.fromRawMap(Map.of("id", 1, "name", "a")),
                    WriteRecord.WriteOperation.INSERT, Instant.now());
            WriteResult result = writer.write("test_table", List.of(record),
                    WriteMode.APPEND, WriteContext.selfManaged());

            assertEquals(1, captured.size());
            assertSame(result, captured.get(0));
            assertEquals("test_table", result.tableName());
            assertEquals(1, result.recordCount());
        }
    }

    @Test
    void nullCallbackDoesNotThrow() {
        RecordingDataFileSink sink = new RecordingDataFileSink(1);
        try (LakeTableWriter writer = new LakeTableWriter(sink, "id", null)) {
            WriteRecord record = new WriteRecord(
                    TypedRow.fromRawMap(Map.of("id", 1)),
                    WriteRecord.WriteOperation.INSERT, Instant.now());
            WriteResult result = writer.write("t", List.of(record),
                    WriteMode.APPEND, WriteContext.selfManaged());

            assertNotNull(result);
            assertEquals(1, result.recordCount());
        }
    }

    static final class RecordingDataFileSink implements DataFileSink {
        private final int filesToReturn;

        RecordingDataFileSink(int filesToReturn) {
            this.filesToReturn = filesToReturn;
        }

        @Override
        public int writeRecords(String tableName, List<WriteRecord> records, WriteMode writeMode) {
            return filesToReturn;
        }

        @Override
        public void close() {}
    }
}
