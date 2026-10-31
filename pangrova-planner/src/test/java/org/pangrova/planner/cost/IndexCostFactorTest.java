package org.pangrova.planner.cost;

import org.pangrova.connector.spi.metadata.table.QualifiedTableName;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for {@link IndexCostFactor} — verifies cost multiplier computation
 * based on index skip ratios (R11.2).
 */
class IndexCostFactorTest {

    @Test
    void noFilterColumns_returnsOne() {
        var factor = new IndexCostFactor((t, c) -> 0.7);
        assertThat(factor.computeCostMultiplier(new QualifiedTableName("test", "db", "t"), Set.of())).isEqualTo(1.0);
        assertThat(factor.computeCostMultiplier(new QualifiedTableName("test", "db", "t"), null)).isEqualTo(1.0);
    }

    @Test
    void noIndexOnColumn_returnsOne() {
        var factor = new IndexCostFactor((t, c) -> 0.0);
        assertThat(factor.computeCostMultiplier(new QualifiedTableName("test", "db", "t"), Set.of("col")))
                .isEqualTo(1.0);
    }

    @Test
    void indexWithSkipRatio_reducesCost() {
        // 70% skip ratio → 30% cost
        var factor = new IndexCostFactor((t, c) -> 0.7);
        assertThat(factor.computeCostMultiplier(new QualifiedTableName("test", "db", "t"), Set.of("col")))
                .isCloseTo(0.3, within(0.001));
    }

    @Test
    void bestSkipRatioUsedAcrossColumns() {
        // col_a: 50% skip, col_b: 80% skip → use 80%
        var factor = new IndexCostFactor((t, c) ->
                "col_b".equals(c) ? 0.8 : 0.5);
        assertThat(factor.computeCostMultiplier(new QualifiedTableName("test", "db", "t"), Set.of("col_a", "col_b")))
                .isCloseTo(0.2, within(0.001));
    }

    @Test
    void costNeverBelowMinimum() {
        // 99% skip ratio → should clamp to MIN_COST_MULTIPLIER
        var factor = new IndexCostFactor((t, c) -> 0.99);
        double result = factor.computeCostMultiplier(new QualifiedTableName("test", "db", "t"), Set.of("col"));
        assertThat(result).isGreaterThanOrEqualTo(IndexCostFactor.MIN_COST_MULTIPLIER);
    }

    @Test
    void fullSkipRatio_clampsToMinimum() {
        var factor = new IndexCostFactor((t, c) -> 1.0);
        assertThat(factor.computeCostMultiplier(new QualifiedTableName("test", "db", "t"), Set.of("col")))
                .isEqualTo(IndexCostFactor.MIN_COST_MULTIPLIER);
    }

    @Test
    void estimateScanCost_withIndexFactor() {
        var collector = new org.pangrova.planner.stats.StatisticsCollector();
        var cs = new org.pangrova.planner.stats.ColumnStatistics(
                10000, 0, 0L, 10000L, 100, 8.0,
                java.util.Optional.empty(), 1.0);
        collector.putStatistics(new QualifiedTableName("test", "db", "t"), new org.pangrova.planner.stats.PlannerTableStatistics(
                new QualifiedTableName("test", "db", "t"), java.util.Map.of("col", cs), java.time.Instant.now()));

        var indexFactor = new IndexCostFactor((t, c) -> 0.6);
        var estimator = new StatisticsBasedCostEstimator(collector, null, indexFactor);

        // 10000 rows × 0.4 (1.0 - 0.6 skip) = 4000
        assertThat(estimator.estimateScanCost(new QualifiedTableName("test", "db", "t"), Set.of("col")))
                .isCloseTo(4000.0, within(1.0));
    }

    @Test
    void estimateScanCost_withoutIndexFactor_returnsRowCount() {
        var collector = new org.pangrova.planner.stats.StatisticsCollector();
        var cs = new org.pangrova.planner.stats.ColumnStatistics(
                10000, 0, 0L, 10000L, 100, 8.0,
                java.util.Optional.empty(), 1.0);
        collector.putStatistics(new QualifiedTableName("test", "db", "t"), new org.pangrova.planner.stats.PlannerTableStatistics(
                new QualifiedTableName("test", "db", "t"), java.util.Map.of("col", cs), java.time.Instant.now()));

        var estimator = new StatisticsBasedCostEstimator(collector);

        assertThat(estimator.estimateScanCost(new QualifiedTableName("test", "db", "t"), Set.of("col")))
                .isCloseTo(10000.0, within(1.0));
    }
}
