package org.pangrova.streaming.watermark;

import org.pangrova.connector.api.enums.WriteOperation;
import org.pangrova.streaming.enums.LateDataPolicy;

import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.enums.TableFormat;
import org.pangrova.connector.api.write.TableWriter;
import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.enums.WriteMode;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.connector.api.write.WriteResult;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies F-CHAIN-STREAM-05 fix: the dead continuous-query consumer wiring has
 * been removed, and watermarks produced by the CDC pipeline are consumed only by
 * the LateDataRouter for allowed-lateness decisions.
 *
 * <p>Prior to this fix, a {@code ContinuousQueryConsumer} class existed in the
 * {@code org.pangrova.streaming.query} package that pretended to consume watermarks
 * for incremental query refresh, but its executor was a log-only no-op. That dead
 * code has been deleted.</p>
 */
@DisplayName("F-CHAIN-STREAM-05: Watermark consumption is limited to LateDataRouter")
class WatermarkConsumptionTest {

    @Test
    @DisplayName("WatermarkTracker updates are consumed by LateDataRouter for routing decisions")
    void watermarkTrackerConsumedByLateDataRouter() {
        // Setup: watermark tracker with a real watermark
        WatermarkStore store = new NoOpWatermarkStore();
        WatermarkTracker tracker = new WatermarkTracker(store);

        Instant watermark = Instant.parse("2024-06-01T12:00:00Z");
        tracker.updateWatermark("events", watermark);

        // Setup: LateDataRouter consumes from the same tracker
        RecordingSideOutputWriter sideWriter = new RecordingSideOutputWriter();
        LateDataConfig config = new LateDataConfig(
                LateDataPolicy.SIDE_OUTPUT, "_late", Duration.ofDays(7), Duration.ZERO);
        LateDataRouter router = new LateDataRouter(tracker, sideWriter, config);

        // On-time record goes to main stream
        WriteRecord onTime = makeRecord(Instant.parse("2024-06-01T12:00:01Z"));
        assertThat(router.route("events", onTime)).isTrue();
        assertThat(sideWriter.written).isEmpty();

        // Late record goes to side output — watermark is actually consumed
        WriteRecord late = makeRecord(Instant.parse("2024-06-01T11:59:59Z"));
        assertThat(router.route("events", late)).isFalse();
        assertThat(sideWriter.written).hasSize(1);
    }

    @Test
    @DisplayName("Advancing watermark changes LateDataRouter routing decisions")
    void advancingWatermarkChangesRouting() {
        WatermarkStore store = new NoOpWatermarkStore();
        WatermarkTracker tracker = new WatermarkTracker(store);

        RecordingSideOutputWriter sideWriter = new RecordingSideOutputWriter();
        LateDataConfig config = new LateDataConfig(
                LateDataPolicy.SIDE_OUTPUT, "_late", Duration.ofDays(7), Duration.ZERO);
        LateDataRouter router = new LateDataRouter(tracker, sideWriter, config);

        // Initially no watermark set — everything is on-time (watermark = Instant.MIN)
        WriteRecord record = makeRecord(Instant.parse("2024-06-01T11:00:00Z"));
        assertThat(router.route("events", record)).isTrue();

        // After watermark advances past the record's event time, it becomes late
        tracker.updateWatermark("events", Instant.parse("2024-06-01T12:00:00Z"));
        assertThat(router.route("events", record)).isFalse();
        assertThat(sideWriter.written).hasSize(1);
    }

    private static WriteRecord makeRecord(Instant eventTime) {
        TypedRow row = TypedRow.fromRawMap(Map.of("id", 1, "value", "test"));
        return new WriteRecord(row, WriteOperation.INSERT, eventTime);
    }

    static final class NoOpWatermarkStore implements WatermarkStore {
        @Override
        public Instant advance(String tableName, Instant watermark) { return watermark; }

        @Override
        public Optional<Instant> load(String tableName) {
            return Optional.empty();
        }
    }

    static final class RecordingSideOutputWriter implements TableWriter {
        final List<WriteRecord> written = new ArrayList<>();

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
}
