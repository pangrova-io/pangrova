package org.pangrova.planner.property.adaptivequery;

import org.pangrova.planner.adaptivequery.ShuffleStageStats;
import java.util.HashMap;
import java.util.Map;

import org.pangrova.shuffle.api.ShufflePartitionStats;

import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Combinators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property-based tests for {@link ShuffleStageStats} partition map immutability.
 *
 * <p><b>Validates: Requirements 2</b></p>
 */
class ShuffleStageStatsPropertyTest {

    /**
     * Property 1: The partition map returned by {@code partitionStats()} is always
     * unmodifiable — put, remove, and clear all throw {@code UnsupportedOperationException},
     * regardless of the input map contents.
     *
     * <p><b>Validates: Requirements 2</b></p>
     */
    @Property(tries = 10)
    @Label("Property 1: partitionStats is always unmodifiable (put, remove, clear)")
    void partitionStatsIsAlwaysUnmodifiable(
            @ForAll("partitionStatsMaps") Map<Integer, ShufflePartitionStats> input) {

        ShuffleStageStats stats = new ShuffleStageStats(
                "stage-1", 1000L, 100L, 500L, 50L, input);

        Map<Integer, ShufflePartitionStats> returned = stats.partitionStats();

        assertThatThrownBy(() -> returned.put(999, new ShufflePartitionStats(999, 1, 1)))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> returned.remove(0))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> returned.clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * Property 2: Mutating the original map after construction does not affect
     * the record's partition stats (defensive copy guarantee).
     */
    @Property(tries = 10)
    @Label("Property 2: original map mutation does not affect record")
    void originalMapMutationDoesNotAffectRecord(
            @ForAll("partitionStatsMaps") Map<Integer, ShufflePartitionStats> input) {

        HashMap<Integer, ShufflePartitionStats> mutable = new HashMap<>(input);
        ShuffleStageStats stats = new ShuffleStageStats(
                "stage-1", 1000L, 100L, 500L, 50L, mutable);

        int sizeBefore = stats.partitionStats().size();
        mutable.put(Integer.MAX_VALUE, new ShufflePartitionStats(Integer.MAX_VALUE, 42, 42));

        assertThat(stats.partitionStats()).hasSize(sizeBefore);
    }

    /**
     * Property 3: Null partition stats input is normalized to an empty
     * unmodifiable map (never null).
     */
    @Property(tries = 10)
    @Label("Property 3: null partitionStats normalized to empty unmodifiable map")
    void nullPartitionStatsNormalizedToEmptyMap(@ForAll("stageIds") String stageId) {
        ShuffleStageStats stats = new ShuffleStageStats(
                stageId, 0L, 0L, 0L, 0L, null);

        assertThat(stats.partitionStats()).isNotNull().isEmpty();
        assertThatThrownBy(() -> stats.partitionStats().put(0, new ShufflePartitionStats(0, 1, 1)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Provide
    Arbitrary<Map<Integer, ShufflePartitionStats>> partitionStatsMaps() {
        Arbitrary<Integer> partitionIds = Arbitraries.integers().between(0, 127);
        Arbitrary<Long> dataBytes = Arbitraries.longs().between(0, 10_000_000_000L);
        Arbitrary<Long> rowCounts = Arbitraries.longs().between(0, 100_000_000L);

        Arbitrary<ShufflePartitionStats> statsArb = Combinators.combine(partitionIds, dataBytes, rowCounts)
                .as(ShufflePartitionStats::new);

        return statsArb.list().ofMinSize(0).ofMaxSize(16)
                .map(list -> {
                    Map<Integer, ShufflePartitionStats> map = new HashMap<>();
                    for (ShufflePartitionStats s : list) {
                        map.put(s.partitionId(), s);
                    }
                    return map;
                });
    }

    @Provide
    Arbitrary<String> stageIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(16)
                .map(s -> "stage-" + s);
    }
}
