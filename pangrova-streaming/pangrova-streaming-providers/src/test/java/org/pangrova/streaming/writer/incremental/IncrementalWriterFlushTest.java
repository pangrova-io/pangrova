package org.pangrova.streaming.writer.incremental;

import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.write.TableFormat;
import org.pangrova.connector.spi.write.TableWriter;
import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.write.WriteMode;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.connector.api.write.WriteResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests IncrementalWriter.flush() data correctness:
 * buffer must not be cleared if write fails.
 */
class IncrementalWriterFlushTest {

    @Test
    void flushClearsBufferOnSuccess() {
        RecordingTableWriter writer = new RecordingTableWriter();
        IncrementalWriter sw = new IncrementalWriter("test_table", writer, largeBufferConfig());
        sw.write(row("a", 1));
        sw.write(row("b", 2));
        assertEquals(2, sw.bufferSize());

        sw.flush();

        assertEquals(0, sw.bufferSize());
        assertEquals(0, sw.bufferBytes());
        assertEquals(1, writer.writeCalls.size());
        assertEquals(2, writer.writeCalls.get(0).size());
        sw.close();
    }

    @Test
    void flushPreservesBufferOnWriteFailure() {
        FailingTableWriter writer = new FailingTableWriter();
        IncrementalWriter sw = new IncrementalWriter("test_table", writer, largeBufferConfig());
        sw.write(row("a", 1));
        sw.write(row("b", 2));
        assertEquals(2, sw.bufferSize());

        assertThrows(RuntimeException.class, sw::flush);

        // Buffer must NOT be cleared after write failure
        assertEquals(2, sw.bufferSize());
    }

    @Test
    void flushAfterRecoveryWritesPreviouslyRetainedRecords() {
        AtomicInteger callCount = new AtomicInteger(0);
        TableWriter writer = new TableWriter() {
            @Override
            public WriteResult write(String tableName, List<WriteRecord> records,
                                     WriteMode writeMode, WriteContext context) {
                if (callCount.getAndIncrement() == 0) {
                    throw new RuntimeException("transient failure");
                }
                return new WriteResult(tableName, records.size(), 1, 0, 0, Instant.now());
            }
            @Override public TableFormat format() { return TableFormat.ICEBERG; }
            @Override public void close() {}
        };

        IncrementalWriter sw = new IncrementalWriter("test_table", writer, largeBufferConfig());
        sw.write(row("a", 1));

        // First flush fails — buffer retained
        assertThrows(RuntimeException.class, sw::flush);
        assertEquals(1, sw.bufferSize());

        // Second flush succeeds — buffer cleared
        sw.flush();
        assertEquals(0, sw.bufferSize());
        sw.close();
    }

    @Test
    void emptyFlushIsNoOp() {
        RecordingTableWriter writer = new RecordingTableWriter();
        IncrementalWriter sw = new IncrementalWriter("test_table", writer, largeBufferConfig());
        sw.flush();
        assertEquals(0, writer.writeCalls.size());
        sw.close();
    }

    private static TypedRow row(String name, int value) {
        return TypedRow.of(Map.of(
                "name", new ColumnValue.StringValue(name),
                "value", new ColumnValue.IntValue(value)));
    }

    private static IncrementalWriterConfig largeBufferConfig() {
        return new IncrementalWriterConfig(1024 * 1024, 10_000, Duration.ofHours(1));
    }

    /** TableWriter that records all write calls. */
    static class RecordingTableWriter implements TableWriter {
        final List<List<WriteRecord>> writeCalls = new ArrayList<>();

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode writeMode, WriteContext context) {
            writeCalls.add(List.copyOf(records));
            return new WriteResult(tableName, records.size(), 1, 0, 0, Instant.now());
        }

        @Override public TableFormat format() { return TableFormat.ICEBERG; }
        @Override public void close() {}
    }

    /** TableWriter that always throws on write. */
    static class FailingTableWriter implements TableWriter {
        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode writeMode, WriteContext context) {
            throw new RuntimeException("simulated write failure");
        }

        @Override public TableFormat format() { return TableFormat.ICEBERG; }
        @Override public void close() {}
    }
}
