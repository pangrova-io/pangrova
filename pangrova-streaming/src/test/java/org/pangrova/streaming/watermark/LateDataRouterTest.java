package org.pangrova.streaming.watermark;

import org.pangrova.spi.streaming.WatermarkStore;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.write.model.TableFormat;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link LateDataRouter} routing logic and exception propagation.
 */
class LateDataRouterTest {

    private InMemoryWatermarkStore watermarkStore;
    private WatermarkTracker watermarkTracker;
    private RecordingSideOutputWriter sideOutputWriter;
    private LateDataRouter router;

    @BeforeEach
    void setUp() {
        watermarkStore = new InMemoryWatermarkStore();
        watermarkTracker = new WatermarkTracker(watermarkStore);
        sideOutputWriter = new RecordingSideOutputWriter();
        LateDataConfig config = new LateDataConfig(
            LateDataPolicy.SIDE_OUTPUT, "_late", null, java.time.Duration.ZERO);
        router = new LateDataRouter(watermarkTracker, sideOutputWriter, config);
    }

    @Test
    void onTimeRecordRoutedToMainStream() {
        Instant watermark = Instant.parse("2024-01-01T10:00:00Z");
        watermarkTracker.updateWatermark("orders", watermark);

        WriteRecord record = makeRecord(Instant.parse("2024-01-01T10:00:01Z"));
        boolean mainStream = router.route("orders", record);

        assertThat(mainStream).isTrue();
        assertThat(sideOutputWriter.written).isEmpty();
        assertThat(router.lateDataTotal()).isZero();
    }

    @Test
    void lateRecordRoutedToSideOutput() {
        Instant watermark = Instant.parse("2024-01-01T10:00:00Z");
        watermarkTracker.updateWatermark("orders", watermark);

        WriteRecord record = makeRecord(Instant.parse("2024-01-01T09:59:59Z"));
        boolean mainStream = router.route("orders", record);

        assertThat(mainStream).isFalse();
        assertThat(sideOutputWriter.written).hasSize(1);
        assertThat(sideOutputWriter.lastTableName).isEqualTo("orders_late");
        assertThat(router.lateDataTotal()).isEqualTo(1);
    }

    @Test
    void disabledRouterAlwaysReturnsMainStream() {
        LateDataConfig disabledConfig = new LateDataConfig(
            LateDataPolicy.DROP, "_late", null, java.time.Duration.ZERO);
        LateDataRouter disabledRouter = new LateDataRouter(
            watermarkTracker, sideOutputWriter, disabledConfig);

        Instant watermark = Instant.parse("2024-01-01T10:00:00Z");
        watermarkTracker.updateWatermark("orders", watermark);

        WriteRecord lateRecord = makeRecord(Instant.parse("2024-01-01T09:00:00Z"));
        boolean mainStream = disabledRouter.route("orders", lateRecord);

        assertThat(mainStream).isTrue();
        assertThat(sideOutputWriter.written).isEmpty();
    }

    @Test
    void sideOutputWriteFailureThrowsLateDataRouteException() {
        sideOutputWriter.failOnWrite = true;
        Instant watermark = Instant.parse("2024-01-01T10:00:00Z");
        watermarkTracker.updateWatermark("orders", watermark);

        WriteRecord lateRecord = makeRecord(Instant.parse("2024-01-01T09:00:00Z"));

        assertThatThrownBy(() -> router.route("orders", lateRecord))
            .isInstanceOf(LateDataRouteException.class)
            .hasMessageContaining("orders_late")
            .hasCauseInstanceOf(RuntimeException.class);

        // Counter should NOT increment on failure
        assertThat(router.lateDataTotal()).isZero();
    }

    @Test
    void sideOutputRecordEnrichedWithMetadata() {
        Instant watermark = Instant.parse("2024-01-01T10:00:00Z");
        watermarkTracker.updateWatermark("orders", watermark);

        WriteRecord lateRecord = makeRecord(Instant.parse("2024-01-01T09:59:00Z"));
        router.route("orders", lateRecord);

        assertThat(sideOutputWriter.written).hasSize(1);
        WriteRecord sideRecord = sideOutputWriter.written.get(0);
        assertThat(sideRecord.data().containsColumn("_arrival_time")).isTrue();
        assertThat(sideRecord.data().containsColumn("_watermark_at_arrival")).isTrue();
    }

    private static WriteRecord makeRecord(Instant eventTime) {
        TypedRow row = TypedRow.fromRawMap(Map.of("id", 1, "name", "test"));
        return new WriteRecord(row, WriteRecord.WriteOperation.INSERT, eventTime);
    }

    /** Recording writer that captures writes for assertions. */
    static final class RecordingSideOutputWriter implements TableWriter {
        final List<WriteRecord> written = new ArrayList<>();
        String lastTableName;
        boolean failOnWrite = false;

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode mode, WriteContext context) {
            if (failOnWrite) {
                throw new RuntimeException("Simulated side-output write failure");
            }
            lastTableName = tableName;
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

    /** Simple in-memory watermark store for tests. */
    static final class InMemoryWatermarkStore implements WatermarkStore {
        @Override
        public void save(String tableName, Instant watermark) {}

        @Override
        public java.util.Optional<Instant> load(String tableName) {
            return java.util.Optional.empty();
        }
    }
}
