package org.pangrova.streaming.cdc;

import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.write.WriteMode;
import org.pangrova.connector.api.write.WriteResult;
import org.pangrova.connector.api.write.TableFormat;
import org.pangrova.connector.spi.write.TableWriter;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.spi.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies CdcConsumer partial failure handling (Task 12.9).
 * Valid records are committed; invalid ones are dead-lettered.
 */
class CdcConsumerPartialFailureTest {

    @Test
    void validMessagesProcessedAndCommitted() {
        List<CdcEnvelope> committed = new ArrayList<>();
        CdcConsumer consumer = createConsumer(committed);

        consumer.processBatch(List.of(validDebeziumMessage("t1", 1)));

        assertEquals(1, committed.size());
        assertEquals("test_db.t1", committed.get(0).tableName());
    }

    @Test
    void invalidMessageSkippedValidMessageCommitted() {
        List<CdcEnvelope> committed = new ArrayList<>();
        CdcConsumer consumer = createConsumer(committed);

        List<String> batch = List.of(
                "NOT_VALID_JSON",
                validDebeziumMessage("t1", 2));

        consumer.processBatch(batch);

        assertEquals(1, committed.size());
        assertEquals("test_db.t1", committed.get(0).tableName());
    }

    @Test
    void allInvalidMessagesResultInNoCommit() {
        List<CdcEnvelope> committed = new ArrayList<>();
        CdcConsumer consumer = createConsumer(committed);

        consumer.processBatch(List.of("bad1", "bad2", "{broken"));

        assertTrue(committed.isEmpty());
    }

    @Test
    void emptyBatchIsNoOp() {
        List<CdcEnvelope> committed = new ArrayList<>();
        CdcConsumer consumer = createConsumer(committed);

        consumer.processBatch(List.of());
        assertTrue(committed.isEmpty());
    }

    private CdcConsumer createConsumer(List<CdcEnvelope> commitSink) {
        CdcConfig config = new CdcConfig(
                "localhost:9092", "test-group", List.of("topic"),
                "consumer-1", "tenant-1", 4, 1024 * 1024);
        return new CdcConsumer(new RecordingTableWriter(), config,
                commitSink::addAll, new TestMetricsProvider());
    }

    private String validDebeziumMessage(String table, int id) {
        return """
                {"payload":{"op":"c","before":null,"after":{"id":%d,"name":"test"},\
                "ts_ms":%d,"source":{"db":"test_db","table":"%s"}}}"""
                .formatted(id, Instant.now().toEpochMilli(), table);
    }

    /** Real minimal TableWriter — records writes without side effects. */
    static final class RecordingTableWriter implements TableWriter {
        private final List<WriteRecord> written = new ArrayList<>();

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode mode, WriteContext context) {
            written.addAll(records);
            return new WriteResult(tableName, records.size(), 1, 0, 0, Instant.now());
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {}
    }

    /** Minimal MetricsProvider for test — avoids pangrova-observability dependency. */
    static final class TestMetricsProvider implements MetricsProvider {
        @Override
        public LongCounter createCounter(String name, String desc, String unit) {
            return value -> {};
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String desc,
                                                  String unit, Supplier<Double> supplier) {
            return () -> { /* This recording fixture retains no callback. */ };
        }

        @Override
        public DoubleHistogram createHistogram(String name, String desc,
                                                String unit, List<Double> boundaries) {
            return value -> {};
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String desc, String unit) {
            return value -> {};
        }
    }
}
