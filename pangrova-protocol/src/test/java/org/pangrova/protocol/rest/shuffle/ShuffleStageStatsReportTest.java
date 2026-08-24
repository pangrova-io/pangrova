package org.pangrova.protocol.rest.shuffle;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ShuffleStageStatsReport} and {@link PartitionStats}.
 *
 * <p><b>Validates: Requirements 1.37, 2.37, 2.39</b></p>
 */
class ShuffleStageStatsReportTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Nested
    class Construction {

        @Test
        void validReportPreservesAllFields() {
            var stats = Map.of(0, new PartitionStats(1024, 100),
                               1, new PartitionStats(2048, 200));
            var report = new ShuffleStageStatsReport(
                    "q-1", "s-1", stats, 3072, 300, 1700000000000L);

            assertThat(report.queryId()).isEqualTo("q-1");
            assertThat(report.stageId()).isEqualTo("s-1");
            assertThat(report.partitionStats()).hasSize(2);
            assertThat(report.shuffleBytes()).isEqualTo(3072);
            assertThat(report.shuffleRows()).isEqualTo(300);
            assertThat(report.timestampMs()).isEqualTo(1700000000000L);
        }

        @Test
        void nullPartitionStatsDefaultsToEmptyMap() {
            var report = new ShuffleStageStatsReport(
                    "q-1", "s-1", null, 0, 0, 0);
            assertThat(report.partitionStats()).isEmpty();
        }

        @Test
        void nullQueryIdThrows() {
            assertThatThrownBy(() -> new ShuffleStageStatsReport(
                    null, "s-1", null, 0, 0, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("queryId");
        }

        @Test
        void blankStageIdThrows() {
            assertThatThrownBy(() -> new ShuffleStageStatsReport(
                    "q-1", "  ", null, 0, 0, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stageId");
        }

        @Test
        void negativeShuffleBytesThrows() {
            assertThatThrownBy(() -> new ShuffleStageStatsReport(
                    "q-1", "s-1", null, -1, 0, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("shuffleBytes");
        }

        @Test
        void negativeShuffleRowsThrows() {
            assertThatThrownBy(() -> new ShuffleStageStatsReport(
                    "q-1", "s-1", null, 0, -1, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("shuffleRows");
        }

        @Test
        void partitionStatsMapIsImmutable() {
            var mutable = new java.util.HashMap<Integer, PartitionStats>();
            mutable.put(0, new PartitionStats(100, 10));
            var report = new ShuffleStageStatsReport(
                    "q-1", "s-1", mutable, 100, 10, 0);

            assertThatThrownBy(() -> report.partitionStats().put(1, new PartitionStats(0, 0)))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    class JsonRoundTrip {

        @Test
        void fullReportRoundTrip() throws Exception {
            var stats = Map.of(0, new PartitionStats(1024, 100));
            var original = new ShuffleStageStatsReport(
                    "q-1", "s-1", stats, 1024, 100, 1700000000000L);

            String json = MAPPER.writeValueAsString(original);
            var deserialized = MAPPER.readValue(json, ShuffleStageStatsReport.class);

            assertThat(deserialized).isEqualTo(original);
        }

        @Test
        void emptyPartitionStatsRoundTrip() throws Exception {
            var original = new ShuffleStageStatsReport(
                    "q-1", "s-1", Map.of(), 0, 0, 0);

            String json = MAPPER.writeValueAsString(original);
            var deserialized = MAPPER.readValue(json, ShuffleStageStatsReport.class);

            assertThat(deserialized).isEqualTo(original);
        }
    }

    @Nested
    class VersionCompatibility {

        @Test
        void unknownFieldsIgnoredOnDeserialization() throws Exception {
            String json = """
                    {
                      "query_id": "q-1",
                      "stage_id": "s-1",
                      "shuffle_bytes": 1024,
                      "shuffle_rows": 100,
                      "timestamp_ms": 0,
                      "future_field": "should-be-ignored",
                      "another_new_field": 42
                    }
                    """;

            var report = MAPPER.readValue(json, ShuffleStageStatsReport.class);

            assertThat(report.queryId()).isEqualTo("q-1");
            assertThat(report.stageId()).isEqualTo("s-1");
            assertThat(report.shuffleBytes()).isEqualTo(1024);
            assertThat(report.shuffleRows()).isEqualTo(100);
            assertThat(report.partitionStats()).isEmpty();
        }

        @Test
        void missingOptionalFieldsUseDefaults() throws Exception {
            String json = """
                    {
                      "query_id": "q-1",
                      "stage_id": "s-1"
                    }
                    """;

            var report = MAPPER.readValue(json, ShuffleStageStatsReport.class);

            assertThat(report.queryId()).isEqualTo("q-1");
            assertThat(report.stageId()).isEqualTo("s-1");
            assertThat(report.shuffleBytes()).isZero();
            assertThat(report.shuffleRows()).isZero();
            assertThat(report.timestampMs()).isZero();
            assertThat(report.partitionStats()).isEmpty();
        }

        @Test
        void oldFormatWithoutPartitionStatsDeserializesSuccessfully() throws Exception {
            // Simulates an old-format message that only has basic fields
            String oldFormatJson = """
                    {
                      "query_id": "q-old",
                      "stage_id": "s-old",
                      "shuffle_bytes": 512,
                      "shuffle_rows": 50,
                      "timestamp_ms": 1600000000000
                    }
                    """;

            var report = MAPPER.readValue(oldFormatJson, ShuffleStageStatsReport.class);

            assertThat(report.queryId()).isEqualTo("q-old");
            assertThat(report.stageId()).isEqualTo("s-old");
            assertThat(report.shuffleBytes()).isEqualTo(512);
            assertThat(report.shuffleRows()).isEqualTo(50);
            assertThat(report.timestampMs()).isEqualTo(1600000000000L);
            assertThat(report.partitionStats()).isEmpty();
        }
    }

    @Nested
    class PartitionStatsTest {

        @Test
        void validPartitionStats() {
            var ps = new PartitionStats(1024, 100);
            assertThat(ps.dataBytes()).isEqualTo(1024);
            assertThat(ps.rowCount()).isEqualTo(100);
        }

        @Test
        void negativeDataBytesThrows() {
            assertThatThrownBy(() -> new PartitionStats(-1, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("dataBytes");
        }

        @Test
        void negativeRowCountThrows() {
            assertThatThrownBy(() -> new PartitionStats(0, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("rowCount");
        }

        @Test
        void roundTrip() throws Exception {
            var original = new PartitionStats(2048, 200);
            String json = MAPPER.writeValueAsString(original);
            var deserialized = MAPPER.readValue(json, PartitionStats.class);
            assertThat(deserialized).isEqualTo(original);
        }
    }
}
