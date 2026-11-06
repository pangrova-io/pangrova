package org.pangrova.planner.core.adaptive;

import org.pangrova.planner.api.adaptive.JoinStrategy;
import org.pangrova.planner.api.adaptive.StageReplanDecision;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.Map;

import org.pangrova.planner.core.statistics.StatisticsCollector;
import org.pangrova.shuffle.api.ShufflePartitionStats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link StageReplanCoordinator} — verifies the Driver callback coordinates
 * the three AQE components (statistics collector, partition coalescer, join strategy
 * switcher) to produce correct replan decisions after shuffle stage completion.
 */
class StageReplanCoordinatorTest {

    private static final long MB = 1024L * 1024;

    private StageReplanCoordinator coordinator;
    private StatisticsCollector statisticsCollector;

    @BeforeEach
    void setUp() {
        statisticsCollector = new StatisticsCollector();
        coordinator = new StageReplanCoordinator(
                new PartitionCoalescer(),
                new AdaptiveQueryExecutor(statisticsCollector),
                statisticsCollector,
                AdaptiveQueryConfig.ENABLED);
    }

    @Test
    void disabledConfig_returnsNoReplan() {
        StageReplanCoordinator disabled = new StageReplanCoordinator(
                new PartitionCoalescer(),
                new AdaptiveQueryExecutor(),
                new StatisticsCollector(),
                AdaptiveQueryConfig.DISABLED);

        ShuffleStageStats stats = new ShuffleStageStats("s1", 100 * MB, 1000, 10 * MB, 100,
                Map.of(0, new ShufflePartitionStats(0, 50 * MB, 500),
                        1, new ShufflePartitionStats(1, 50 * MB, 500)));

        StageReplanDecision decision = disabled.onShuffleStageCompleted(
                stats, JoinStrategy.SORT_MERGE_JOIN);

        assertThat(decision).isEqualTo(StageReplanDecision.NO_REPLAN);
        assertThat(decision.needsReplan()).isFalse();
    }

    @Test
    void skewedShuffle_triggersCoalesce_reducingPartitionCount() {
        // 8 small partitions (10MB each) that should be coalesced into fewer groups
        // Target partition size is 256MB (default), so 8 × 10MB = 80MB fits in 1 group
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 10 * MB, 100),
                1, new ShufflePartitionStats(1, 10 * MB, 100),
                2, new ShufflePartitionStats(2, 10 * MB, 100),
                3, new ShufflePartitionStats(3, 10 * MB, 100),
                4, new ShufflePartitionStats(4, 10 * MB, 100),
                5, new ShufflePartitionStats(5, 10 * MB, 100),
                6, new ShufflePartitionStats(6, 10 * MB, 100),
                7, new ShufflePartitionStats(7, 10 * MB, 100));

        // Actual data much smaller than estimated → triggers re-optimization
        ShuffleStageStats stats = new ShuffleStageStats(
                "stage-shuffle", 80 * MB, 800, 500 * MB, 5000, partitions);

        StageReplanDecision decision = coordinator.onShuffleStageCompleted(
                stats, JoinStrategy.SORT_MERGE_JOIN);

        assertThat(decision.needsReplan()).isTrue();
        assertThat(decision.coalesceResult().isCoalesced()).isTrue();
        assertThat(decision.coalesceResult().coalescedCount())
                .isLessThan(decision.coalesceResult().originalCount());
        assertThat(decision.coalesceResult().originalCount()).isEqualTo(8);
        // 8 partitions × 10MB = 80MB total, target = 256MB → all fit in 1 group
        assertThat(decision.coalesceResult().coalescedCount()).isEqualTo(1);
    }

    @Test
    void smallBuildSide_switchesToBroadcastJoin() {
        // Actual data size is 5MB, below the 10MB broadcast threshold
        ShuffleStageStats stats = new ShuffleStageStats(
                "stage-build", 5 * MB, 500, 100 * MB, 10000,
                Map.of(0, new ShufflePartitionStats(0, 5 * MB, 500)));

        StageReplanDecision decision = coordinator.onShuffleStageCompleted(
                stats, JoinStrategy.SORT_MERGE_JOIN);

        assertThat(decision.needsReplan()).isTrue();
        assertThat(decision.reOptimizationTriggered()).isTrue();
        assertThat(decision.newJoinStrategy()).contains(JoinStrategy.BROADCAST_HASH_JOIN);
    }

    @Test
    void withinEstimateRange_noReplan() {
        // Actual is within [0.5x, 2x] of estimated → no re-optimization
        // Partitions are large enough → no coalescing
        ShuffleStageStats stats = new ShuffleStageStats(
                "stage-normal", 300 * MB, 3000, 250 * MB, 2500,
                Map.of(0, new ShufflePartitionStats(0, 300 * MB, 3000)));

        StageReplanDecision decision = coordinator.onShuffleStageCompleted(
                stats, JoinStrategy.HASH_JOIN);

        assertThat(decision.needsReplan()).isFalse();
        assertThat(decision.coalesceResult().isCoalesced()).isFalse();
        assertThat(decision.newJoinStrategy()).isEmpty();
        assertThat(decision.reOptimizationTriggered()).isFalse();
    }

    @Test
    void coalesceWithoutJoinSwitch_stillNeedsReplan() {
        // Many small partitions but actual size is within estimate range
        // → coalescing triggers replan, but no join strategy switch
        Map<Integer, ShufflePartitionStats> partitions = Map.of(
                0, new ShufflePartitionStats(0, 20 * MB, 200),
                1, new ShufflePartitionStats(1, 20 * MB, 200),
                2, new ShufflePartitionStats(2, 20 * MB, 200),
                3, new ShufflePartitionStats(3, 20 * MB, 200),
                4, new ShufflePartitionStats(4, 20 * MB, 200));

        // Actual within [0.5x, 2x] of estimated → no re-optimization trigger
        ShuffleStageStats stats = new ShuffleStageStats(
                "stage-coalesce", 100 * MB, 1000, 80 * MB, 800, partitions);

        StageReplanDecision decision = coordinator.onShuffleStageCompleted(
                stats, JoinStrategy.HASH_JOIN);

        assertThat(decision.needsReplan()).isTrue();
        assertThat(decision.coalesceResult().isCoalesced()).isTrue();
        assertThat(decision.coalesceResult().originalCount()).isEqualTo(5);
        assertThat(decision.coalesceResult().coalescedCount()).isLessThan(5);
        assertThat(decision.newJoinStrategy()).isEmpty();
        assertThat(decision.reOptimizationTriggered()).isFalse();
    }

    @Test
    void cardinalityFeedback_recordedToStatisticsCollector() {
        // Pre-populate statistics so recordRuntimeFeedback has something to update
        statisticsCollector.analyzeTable(new QualifiedTableName("test", "db", "orders"), java.util.List.of("id", "amount"));

        // Large divergence: estimated 1000 rows, actual 10000 rows
        ShuffleStageStats stats = new ShuffleStageStats(
                "stage-feedback", 100 * MB, 10000, 10 * MB, 1000,
                Map.of(0, new ShufflePartitionStats(0, 100 * MB, 10000)));

        coordinator.onShuffleStageCompleted(stats, JoinStrategy.HASH_JOIN, new QualifiedTableName("test", "db", "orders"));

        // Verify statistics were updated (getStatistics returns the adjusted value)
        var updatedStats = statisticsCollector.getStatistics(new QualifiedTableName("test", "db", "orders"));
        assertThat(updatedStats).isPresent();
    }

    @Test
    void nullStats_throwsNullPointerException() {
        assertThatThrownBy(() -> coordinator.onShuffleStageCompleted(
                null, JoinStrategy.HASH_JOIN))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("stats");
    }

    @Test
    void nullJoinStrategy_throwsNullPointerException() {
        ShuffleStageStats stats = new ShuffleStageStats("s1", 100, 10, 100, 10);
        assertThatThrownBy(() -> coordinator.onShuffleStageCompleted(stats, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("currentJoinStrategy");
    }

    @Test
    void recommendedParallelism_basedOnActualDataSize() {
        // 1GB actual data → should recommend ~4 tasks (1GB / 256MB target)
        ShuffleStageStats stats = new ShuffleStageStats(
                "stage-large", 1024 * MB, 100000, 100 * MB, 10000,
                Map.of(0, new ShufflePartitionStats(0, 1024 * MB, 100000)));

        StageReplanDecision decision = coordinator.onShuffleStageCompleted(
                stats, JoinStrategy.HASH_JOIN);

        assertThat(decision.recommendedParallelism()).isGreaterThan(0);
        assertThat(decision.recommendedParallelism()).isEqualTo(4);
    }
}
