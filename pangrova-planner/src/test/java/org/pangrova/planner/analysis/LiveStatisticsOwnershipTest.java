package org.pangrova.planner.analysis;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import org.junit.jupiter.api.Test;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.AdaptiveQueryExecutor;
import org.pangrova.planner.api.enums.JoinStrategy;
import org.pangrova.planner.adaptive.PartitionCoalescer;
import org.pangrova.planner.adaptive.ShuffleStageStats;
import org.pangrova.planner.adaptive.StageReplanCoordinator;
import org.pangrova.planner.cost.StatisticsBasedCostEstimator;
import org.pangrova.planner.statistics.CardinalityFeedbackStore;
import org.pangrova.planner.api.statistics.ColumnStatistics;
import org.pangrova.planner.statistics.PlannerTableStatistics;
import org.pangrova.planner.statistics.StatisticsCollector;

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
        assertThat(estimator.estimateRowCount(new QualifiedTableName("test", "db", "documents"))).isEqualTo(100);
        statistics.putStatistics(new QualifiedTableName("test", "db", "documents"), snapshot(200));
        assertThat(estimator.estimateRowCount(new QualifiedTableName("test", "db", "documents"))).isEqualTo(200);
        feedback.record(new QualifiedTableName("test", "db", "documents").canonicalKey(), 200, 1000);
        assertThat(estimator.estimateRowCount(new QualifiedTableName("test", "db", "documents"))).isEqualTo(1000);
    }

    @Test
    void stageCompletionPublishesToTheSuppliedStatisticsAndFeedbackStores() {
        var statistics = statistics(100);
        var feedback = new CardinalityFeedbackStore();
        var coordinator = new StageReplanCoordinator(new PartitionCoalescer(),
                new AdaptiveQueryExecutor(statistics), statistics, feedback, AdaptiveQueryConfig.ENABLED, null);
        coordinator.onShuffleStageCompleted(new ShuffleStageStats("stage", 1000, 1000, 100, 100),
                JoinStrategy.HASH_JOIN, new QualifiedTableName("test", "db", "documents"));
        assertThat(feedback.correctionFactor(new QualifiedTableName("test", "db", "documents").canonicalKey())).hasValue(10);
        assertThat(statistics.getStatistics(new QualifiedTableName("test", "db", "documents")).orElseThrow()
                .columns().get("id").rowCount()).isGreaterThan(100);
    }

    @Test
    void adaptiveExecutorPublishesToTheSuppliedStatisticsStore() {
        var statistics = statistics(100);
        var executor = new AdaptiveQueryExecutor(statistics);
        executor.processStageCompletion(new ShuffleStageStats("stage", 1000, 1000, 100, 100),
                JoinStrategy.HASH_JOIN, 1024, new QualifiedTableName("test", "db", "documents"));
        assertThat(statistics.getStatistics(new QualifiedTableName("test", "db", "documents")).orElseThrow()
                .columns().get("id").rowCount()).isGreaterThan(100);
    }

    private static StatisticsCollector statistics(long rows) {
        var statistics = new StatisticsCollector();
        statistics.putStatistics(new QualifiedTableName("test", "db", "documents"), snapshot(rows));
        return statistics;
    }

    private static PlannerTableStatistics snapshot(long rows) {
        return new PlannerTableStatistics(new QualifiedTableName("test", "db", "documents"), Map.of("id", new ColumnStatistics(
                rows, 0, 0L, 10L, 10, 8, Optional.empty(), 1)), Instant.EPOCH);
    }
}
