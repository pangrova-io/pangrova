package org.pangrova.streaming.cdc;

import org.junit.jupiter.api.Test;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcRangeAllocatorTest {

    private static final TopicPartitionOffset ORDERS_0 =
            new TopicPartitionOffset("orders", 0);
    private static final TopicPartitionOffset ORDERS_1 =
            new TopicPartitionOffset("orders", 1);

    @Test
    void allocatesContiguousBoundedRangesWithoutChangingSourceOffsets() {
        CdcRangeAllocator allocator = new CdcRangeAllocator(
                Map.of(ORDERS_0, 10L, ORDERS_1, 20L),
                Map.of(ORDERS_0, 17L, ORDERS_1, 22L), 3);

        assertThat(allocator.allocate()).containsExactly(
                new CdcTaskRange("orders", 0, 10, 13),
                new CdcTaskRange("orders", 0, 13, 16),
                new CdcTaskRange("orders", 0, 16, 17),
                new CdcTaskRange("orders", 1, 20, 22));
    }

    @Test
    void groupsRangesIntoDeterministicWorkerTasks() {
        CdcRangeAllocator allocator = new CdcRangeAllocator(
                Map.of(ORDERS_0, 10L, ORDERS_1, 20L),
                Map.of(ORDERS_0, 17L, ORDERS_1, 22L), 3);

        List<List<CdcTaskRange>> tasks = allocator.tasks(2);

        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0)).extracting(CdcTaskRange::partition)
                .containsExactly(0, 0);
        assertThat(tasks.get(1)).extracting(CdcTaskRange::partition)
                .containsExactly(0, 1);
    }

    @Test
    void emptyAdmittedIntervalProducesNoWork() {
        CdcRangeAllocator allocator = new CdcRangeAllocator(
                Map.of(ORDERS_0, 10L), Map.of(ORDERS_0, 10L), 5);

        assertThat(allocator.allocate()).isEmpty();
        assertThat(allocator.tasks(4)).isEmpty();
    }

    @Test
    void rejectsMissingPartitionsAndInvertedIntervals() {
        assertThatThrownBy(() -> new CdcRangeAllocator(
                Map.of(ORDERS_0, 10L), Map.of(ORDERS_1, 20L), 5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CdcRangeAllocator(
                Map.of(ORDERS_0, 10L), Map.of(ORDERS_0, 9L), 5))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
