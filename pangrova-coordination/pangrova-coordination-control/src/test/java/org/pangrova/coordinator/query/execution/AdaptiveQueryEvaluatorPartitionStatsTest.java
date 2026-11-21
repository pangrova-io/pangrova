package org.pangrova.coordinator.query.execution;

import java.util.Map;

import org.pangrova.coordinator.query.execution.dispatch.adaptive.AdaptiveQueryEvaluator;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.adaptive.AdaptiveQueryExecutor;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.ShuffleStageStats;
import org.pangrova.protocol.rest.shuffle.PartitionStats;
import org.pangrova.protocol.rest.shuffle.ShuffleStageStatsReport;
import org.pangrova.shuffle.api.ShufflePartitionStats;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies {@link AdaptiveQueryEvaluator#buildStatsFromReport} correctly maps
 * protocol DTOs to SPI contracts for partition-level AQE.
 *
 * <p><b>Validates: Requirements REQ-1, REQ-2, REQ-3</b></p>
 */
class AdaptiveQueryEvaluatorPartitionStatsTest {

    @Test
    void configuredDetectorReceivesReportedStatsAndControlsStoredDecision() {
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        org.pangrova.shuffle.api.SkewDetector detector = (stats, config) -> {
            calls.incrementAndGet();
            assertThat(stats).containsEntry(1, new ShufflePartitionStats(1, 2000, 20));
            assertThat(config.skewThresholdRatio()).isEqualTo(5.0);
            assertThat(config.targetPartitionBytes()).isEqualTo(256L * 1024 * 1024);
            return new org.pangrova.shuffle.api.SkewDetector.SkewDetectionResult(
                    java.util.List.of(new org.pangrova.shuffle.api.SkewDetector.SkewedPartition(
                            1, 2000, 20, 2)), 1500, 2000);
        };
        var evaluator = new AdaptiveQueryEvaluator(detector,
                AdaptiveQueryConfig.ENABLED, new AdaptiveQueryExecutor());
        var report = new ShuffleStageStatsReport("q-test", "1", Map.of(
                0, new PartitionStats(1000, 10), 1, new PartitionStats(2000, 20)),
                3000, 30, 1);
        var stage = createStageWithReport(1, 2, report);
        var decision = evaluator.evaluateStageCompletion(stage);
        assertThat(calls.get()).isEqualTo(1);
        assertThat(decision.skewResult().skewedPartitions()).containsExactly(1);
        assertThat(decision.skewResult().medianBytes()).isEqualTo(1500);
        assertThat(stage.adaptiveDecision()).isSameAs(decision);
    }

    @Test
    void missingConfiguredDetectorFailsConstruction() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new AdaptiveQueryEvaluator(
                null, AdaptiveQueryConfig.ENABLED, new AdaptiveQueryExecutor()))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("detector");
    }

    @Test
    void buildStatsFromReport_withPartitionStats_mapsCorrectly() {
        AdaptiveQueryEvaluator handler = new AdaptiveQueryEvaluator(
                new org.pangrova.shuffle.skew.MedianSkewDetector(),
                AdaptiveQueryConfig.ENABLED, new AdaptiveQueryExecutor());

        ShuffleStageStatsReport report = new ShuffleStageStatsReport(
                "q-1", "1",
                Map.of(0, new PartitionStats(1000, 10),
                       1, new PartitionStats(2000, 20),
                       2, new PartitionStats(3000, 30)),
                6000L, 60L, System.currentTimeMillis());
        Stage stage = createStageWithReport(1, 3, report);

        ShuffleStageStats stats = handler.buildStatsFromReport(stage);

        assertThat(stats.stageId()).isEqualTo("1");
        assertThat(stats.actualDataSize()).isEqualTo(6000L);
        assertThat(stats.actualRowCount()).isEqualTo(60L);
        assertThat(stats.partitionStats()).hasSize(3);

        ShufflePartitionStats p0 = stats.partitionStats().get(0);
        assertThat(p0.partitionId()).isEqualTo(0);
        assertThat(p0.dataBytes()).isEqualTo(1000);
        assertThat(p0.rowCount()).isEqualTo(10);

        ShufflePartitionStats p2 = stats.partitionStats().get(2);
        assertThat(p2.dataBytes()).isEqualTo(3000);
        assertThat(p2.rowCount()).isEqualTo(30);
    }

    @Test
    void buildStatsFromReport_emptyPartitionStats_returnsEmptyMap() {
        AdaptiveQueryEvaluator handler = new AdaptiveQueryEvaluator(
                new org.pangrova.shuffle.skew.MedianSkewDetector(),
                AdaptiveQueryConfig.ENABLED, new AdaptiveQueryExecutor());

        ShuffleStageStatsReport report = new ShuffleStageStatsReport(
                "q-1", "1", Map.of(), 5000L, 50L, System.currentTimeMillis());
        Stage stage = createStageWithReport(1, 4, report);

        ShuffleStageStats stats = handler.buildStatsFromReport(stage);

        assertThat(stats.actualDataSize()).isEqualTo(5000L);
        assertThat(stats.partitionStats()).isEmpty();
    }

    @Test
    void buildStatsFromReport_noReport_returnsZeroStats() {
        AdaptiveQueryEvaluator handler = new AdaptiveQueryEvaluator(
                new org.pangrova.shuffle.skew.MedianSkewDetector(),
                AdaptiveQueryConfig.ENABLED, new AdaptiveQueryExecutor());

        Stage stage = createStage(1, 4);

        ShuffleStageStats stats = handler.buildStatsFromReport(stage);

        assertThat(stats.actualDataSize()).isZero();
        assertThat(stats.actualRowCount()).isZero();
        assertThat(stats.partitionStats()).isEmpty();
    }

    @Test
    void evaluateStageCompletion_withPartitionStats_completesSuccessfully() {
        AdaptiveQueryEvaluator handler = new AdaptiveQueryEvaluator(
                new org.pangrova.shuffle.skew.MedianSkewDetector(),
                AdaptiveQueryConfig.ENABLED, new AdaptiveQueryExecutor());

        long mb = 1024L * 1024;
        ShuffleStageStatsReport report = new ShuffleStageStatsReport(
                "q-1", "1",
                Map.of(0, new PartitionStats(10 * mb, 100),
                       1, new PartitionStats(10 * mb, 100),
                       2, new PartitionStats(10 * mb, 100)),
                30 * mb, 300L, System.currentTimeMillis());
        Stage stage = createStageWithReport(1, 3, report);

        assertThatCode(() -> handler.evaluateStageCompletion(stage))
                .doesNotThrowAnyException();
    }

    @Test
    void evaluateStageCompletion_skewedPartitions_completesSuccessfully() {
        AdaptiveQueryEvaluator handler = new AdaptiveQueryEvaluator(
                new org.pangrova.shuffle.skew.MedianSkewDetector(),
                AdaptiveQueryConfig.ENABLED, new AdaptiveQueryExecutor());

        long mb = 1024L * 1024;
        // Partition 0 is heavily skewed (2GB vs 50MB median)
        ShuffleStageStatsReport report = new ShuffleStageStatsReport(
                "q-skew", "2",
                Map.of(0, new PartitionStats(2048 * mb, 100_000),
                       1, new PartitionStats(50 * mb, 500),
                       2, new PartitionStats(50 * mb, 500),
                       3, new PartitionStats(50 * mb, 500)),
                2198 * mb, 101_500L, System.currentTimeMillis());
        Stage stage = createStageWithReport(2, 4, report);

        var decision = handler.evaluateStageCompletion(stage);
        assertThat(decision.skewResult().skewedPartitions()).containsExactly(0);
        assertThat(decision.skewResult().maxBytes()).isEqualTo(2048 * mb);
        assertThat(stage.adaptiveDecision()).isSameAs(decision);
    }

    // ── Helpers ──

    private static Stage createStage(int stageId, int parallelism) {
        StageNode plan = createStageNode(stageId, parallelism);
        return Stage.create(plan, "q-test");
    }

    private static Stage createStageWithReport(int stageId, int parallelism,
                                                ShuffleStageStatsReport report) {
        Stage stage = createStage(stageId, parallelism);
        stage.setShuffleStatsReport(report);
        return stage;
    }

    private static StageNode createStageNode(int stageId, int parallelism) {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
        RelNode dummyRel = LogicalValues.createEmpty(cluster, typeFactory.builder().build());
        var fragment = new org.pangrova.planner.analysis.PlanFragment(dummyRel, -1L, -1L);
        return new StageNode(stageId, fragment, parallelism, "COMPUTE", java.util.List.of());
    }
}
