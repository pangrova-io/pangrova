package org.pangrova.streaming.api.commit;

import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.streaming.api.commit.CdcBatchCheckpoint;
import org.pangrova.streaming.api.commit.OffsetRange;

class CdcBatchCheckpointTest {
    @Test
    void capturesEveryTopicPartitionAndActualStart() {
        var checkpoint = CdcBatchCheckpoint.fromRecords(List.of(
                record("orders", 0, 10), record("orders", 0, 14),
                record("orders", 2, 80), record("payments", 0, 7)));
        assertThat(checkpoint.ranges()).containsExactly(
                new OffsetRange("orders", 0, 10, 14),
                new OffsetRange("orders", 2, 80, 80),
                new OffsetRange("payments", 0, 7, 7));
        assertThat(checkpoint.nextOffsets()).isEqualTo(Map.of(
                new TopicPartitionOffset("orders", 0), 15L,
                new TopicPartitionOffset("orders", 2), 81L,
                new TopicPartitionOffset("payments", 0), 8L));
    }

    @Test
    void identityIncludesStartsAndTopicsButIgnoresPartitionIterationOrder() {
        var first = new OffsetRange("orders", 0, 10, 14);
        var second = new OffsetRange("payments", 0, 20, 24);
        var input = new ArrayList<>(List.of(first, second));
        var checkpoint = new CdcBatchCheckpoint(input);
        input.clear();
        assertThat(checkpoint.fingerprint()).isEqualTo(
                new CdcBatchCheckpoint(List.of(second, first)).fingerprint());
        assertThat(checkpoint.fingerprint()).isNotEqualTo(new CdcBatchCheckpoint(List.of(
                new OffsetRange("orders", 0, 11, 14), second)).fingerprint());
        assertThat(checkpoint.fingerprint()).isNotEqualTo(new CdcBatchCheckpoint(List.of(
                new OffsetRange("other", 0, 10, 14), second)).fingerprint());
    }

    @Test
    void rejectsAmbiguousOrUnrepresentableCheckpoints() {
        assertThatThrownBy(() -> new CdcBatchCheckpoint(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CdcBatchCheckpoint(List.of(
                new OffsetRange("orders", 0, 0, 1), new OffsetRange("orders", 0, 2, 3))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OffsetRange("orders", 0, 0, Long.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OffsetRange(" ", 0, 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static SourceRecord record(String topic, int partition, long offset) {
        return new SourceRecord(topic, partition, offset, null, new byte[0], 0);
    }
}
