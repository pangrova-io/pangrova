package org.pangrova.planner;

import org.junit.jupiter.api.Test;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.AdaptiveQueryExecutor;
import org.pangrova.planner.adaptive.JoinStrategy;
import org.pangrova.planner.adaptive.PartitionCoalescer;
import org.pangrova.planner.adaptive.ShuffleStageStats;
import org.pangrova.planner.adaptive.StageReplanCoordinator;
import org.pangrova.planner.cost.StatisticsBasedCostEstimator;
import org.pangrova.planner.stats.CardinalityFeedbackStore;
import org.pangrova.planner.stats.ColumnStatistics;
import org.pangrova.planner.stats.PlannerTableStatistics;
import org.pangrova.planner.stats.StatisticsCollector;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LiveStatisticsOwnershipTest {
    @Test
    void estimatesObserveUpdatesThroughTheOriginalStatisticsAndFeedbackOwners() {
        var statistics = statistics(100);
        var feedback = new CardinalityFeedbackStore();
        var estimator = new StatisticsBasedCostEstimator(statistics, null, null, feedback);
        assertThat(estimator.estimateRowCount("documents")).isEqualTo(100);
        statistics.putStatistics("documents", snapshot(200));
        assertThat(estimator.estimateRowCount("documents")).isEqualTo(200);
        feedback.record("documents", 200, 1000);
        assertThat(estimator.estimateRowCount("documents")).isEqualTo(1000);
    }

    @Test
    void stageCompletionPublishesToTheSuppliedStatisticsAndFeedbackStores() {
        var statistics = statistics(100);
        var feedback = new CardinalityFeedbackStore();
        var coordinator = new StageReplanCoordinator(new PartitionCoalescer(),
                new AdaptiveQueryExecutor(statistics), statistics, feedback, AdaptiveQueryConfig.ENABLED, null);
        coordinator.onShuffleStageCompleted(new ShuffleStageStats("stage", 1000, 1000, 100, 100),
                JoinStrategy.HASH_JOIN, "documents");
        assertThat(feedback.correctionFactor("documents")).hasValue(10);
        assertThat(statistics.getStatistics("documents").orElseThrow()
                .columns().get("id").rowCount()).isGreaterThan(100);
    }

    @Test
    void adaptiveExecutorPublishesToTheSuppliedStatisticsStore() {
        var statistics = statistics(100);
        var executor = new AdaptiveQueryExecutor(statistics);
        executor.processStageCompletion(new ShuffleStageStats("stage", 1000, 1000, 100, 100),
                JoinStrategy.HASH_JOIN, 1024, "documents");
        assertThat(statistics.getStatistics("documents").orElseThrow()
                .columns().get("id").rowCount()).isGreaterThan(100);
    }

    private static StatisticsCollector statistics(long rows) {
        var statistics = new StatisticsCollector();
        statistics.putStatistics("documents", snapshot(rows));
        return statistics;
    }

    private static PlannerTableStatistics snapshot(long rows) {
        return new PlannerTableStatistics("documents", Map.of("id", new ColumnStatistics(
                rows, 0, 0L, 10L, 10, 8, Optional.empty(), 1)), Instant.EPOCH);
    }
}
