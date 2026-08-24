package org.pangrova.shuffle.scheduling;

import java.util.List;
import java.util.Optional;

import org.pangrova.shuffle.scheduling.AdaptiveRepartitionTrigger.RepartitionDecision;
import org.pangrova.shuffle.ShufflePartitionStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link AdaptiveRepartitionTrigger} and {@link RepartitionDecision}.
 */
class AdaptiveRepartitionTriggerTest {

    @Test
    void repartitionDecision_coalesceCreatesValidDecision() {
        RepartitionDecision decision = RepartitionDecision.coalesce(4);

        assertThat(decision.newPartitionCount()).isEqualTo(4);
        assertThat(decision.reason()).contains("coalesce");
    }

    @Test
    void repartitionDecision_splitSkewCreatesValidDecision() {
        RepartitionDecision decision = RepartitionDecision.splitSkew(32);

        assertThat(decision.newPartitionCount()).isEqualTo(32);
        assertThat(decision.reason()).contains("split");
    }

    @Test
    void repartitionDecision_zeroPartitionsThrows() {
        assertThatThrownBy(() -> new RepartitionDecision(0, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("newPartitionCount");
    }

    @Test
    void repartitionDecision_negativePartitionsThrows() {
        assertThatThrownBy(() -> new RepartitionDecision(-1, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("newPartitionCount");
    }

    @Test
    void repartitionDecision_nullReasonThrows() {
        assertThatThrownBy(() -> new RepartitionDecision(4, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reason");
    }

    @Test
    void repartitionDecision_blankReasonThrows() {
        assertThatThrownBy(() -> new RepartitionDecision(4, "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reason");
    }

    @Test
    void repartitionDecision_recordSemantics() {
        RepartitionDecision a = new RepartitionDecision(8, "coalesce small");
        RepartitionDecision b = new RepartitionDecision(8, "coalesce small");

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void triggerImplementation_returnsEmptyForAcceptableDistribution() {
        AdaptiveRepartitionTrigger trigger = (stageId, stats, parallelism) ->
                Optional.empty();

        Optional<RepartitionDecision> result = trigger.evaluate(
                "stage-0",
                List.of(new ShufflePartitionStats(0, 1024, 100)),
                8);

        assertThat(result).isEmpty();
    }

    @Test
    void triggerImplementation_returnsCoalesceForSmallPartitions() {
        AdaptiveRepartitionTrigger trigger = (stageId, stats, parallelism) -> {
            long totalBytes = stats.stream().mapToLong(ShufflePartitionStats::dataBytes).sum();
            long avgBytes = totalBytes / Math.max(stats.size(), 1);
            // If average partition is < 1MB, coalesce
            if (avgBytes < 1_000_000 && parallelism > 1) {
                int newCount = Math.max(1, (int) (totalBytes / 1_000_000));
                return Optional.of(RepartitionDecision.coalesce(Math.max(newCount, 1)));
            }
            return Optional.empty();
        };

        // 4 partitions with 200KB each = 800KB total -> coalesce to 1
        List<ShufflePartitionStats> stats = List.of(
                new ShufflePartitionStats(0, 200_000, 50),
                new ShufflePartitionStats(1, 200_000, 50),
                new ShufflePartitionStats(2, 200_000, 50),
                new ShufflePartitionStats(3, 200_000, 50));

        Optional<RepartitionDecision> result = trigger.evaluate("stage-1", stats, 4);

        assertThat(result).isPresent();
        assertThat(result.get().newPartitionCount()).isEqualTo(1);
        assertThat(result.get().reason()).contains("coalesce");
    }

    @Test
    void triggerImplementation_returnsSplitForSkewedPartitions() {
        AdaptiveRepartitionTrigger trigger = (stageId, stats, parallelism) -> {
            long totalBytes = stats.stream().mapToLong(ShufflePartitionStats::dataBytes).sum();
            long avgBytes = totalBytes / Math.max(stats.size(), 1);
            long maxBytes = stats.stream().mapToLong(ShufflePartitionStats::dataBytes).max().orElse(0);
            // If largest partition is > 3x average, split
            if (maxBytes > 3 * avgBytes && stats.size() > 1) {
                return Optional.of(RepartitionDecision.splitSkew(parallelism * 2));
            }
            return Optional.empty();
        };

        List<ShufflePartitionStats> stats = List.of(
                new ShufflePartitionStats(0, 1_000_000, 1000),
                new ShufflePartitionStats(1, 100_000, 100),
                new ShufflePartitionStats(2, 100_000, 100),
                new ShufflePartitionStats(3, 100_000, 100));

        Optional<RepartitionDecision> result = trigger.evaluate("stage-2", stats, 4);

        assertThat(result).isPresent();
        assertThat(result.get().newPartitionCount()).isEqualTo(8);
        assertThat(result.get().reason()).contains("split");
    }
}
