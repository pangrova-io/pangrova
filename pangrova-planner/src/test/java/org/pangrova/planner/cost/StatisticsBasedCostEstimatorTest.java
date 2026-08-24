package org.pangrova.planner.cost;

import org.pangrova.planner.stats.ColumnStatistics;
import org.pangrova.planner.stats.StatisticsCollector;
import org.pangrova.planner.stats.PlannerTableStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class StatisticsBasedCostEstimatorTest {

    private StatisticsCollector collector;
    private StatisticsBasedCostEstimator estimator;

    @BeforeEach
    void setUp() {
        collector = new StatisticsCollector();
        estimator = new StatisticsBasedCostEstimator(collector);
    }

    private static ColumnStatistics confident(long rowCount, long ndv) {
        return new ColumnStatistics(rowCount, 0, 0L, rowCount, ndv, 8.0, Optional.empty(), 1.0);
    }

    private static ColumnStatistics lowConfidence(long rowCount, long ndv) {
        return new ColumnStatistics(rowCount, 0, 0L, rowCount, ndv, 8.0, Optional.empty(), 0.5);
    }

    @Test
    void confidenceWeightedCardinality_withStatistics() {
        ColumnStatistics cs = confident(5000, 500);
        collector.putStatistics("orders", new PlannerTableStatistics(
                "orders", Map.of("id", cs), Instant.now()));

        double expected = cs.rowCount() * cs.confidenceScore();
        assertThat(estimator.confidenceWeightedCardinality("orders"))
                .isCloseTo(expected, within(0.01));
    }

    @Test
    void confidenceWeightedCardinality_noStatistics_returnsDefault() {
        double result = estimator.confidenceWeightedCardinality("unknown_table");
        assertThat(result).isCloseTo(
                StatisticsBasedCostEstimator.DEFAULT_ROW_COUNT * 0.1, within(0.01));
    }

    @Test
    void estimateRowCount_withConfidentStatistics() {
        ColumnStatistics cs = confident(5000, 500);
        collector.putStatistics("users", new PlannerTableStatistics(
                "users", Map.of("name", cs), Instant.now()));

        assertThat(estimator.estimateRowCount("users"))
                .isCloseTo(5000.0, within(0.01));
    }

    @Test
    void estimateRowCount_lowConfidence_returnsDefault() {
        ColumnStatistics cs = lowConfidence(1000, 100);
        collector.putStatistics("users", new PlannerTableStatistics(
                "users", Map.of("name", cs), Instant.now()));

        assertThat(estimator.estimateRowCount("users"))
                .isEqualTo(StatisticsBasedCostEstimator.DEFAULT_ROW_COUNT);
    }

    @Test
    void estimateRowCount_noStatistics_returnsDefault() {
        assertThat(estimator.estimateRowCount("missing"))
                .isEqualTo(StatisticsBasedCostEstimator.DEFAULT_ROW_COUNT);
    }

    @Test
    void estimateSelectivity_withConfidentStatistics() {
        ColumnStatistics cs = confident(10000, 50);
        collector.putStatistics("products", new PlannerTableStatistics(
                "products", Map.of("category", cs), Instant.now()));

        assertThat(estimator.estimateSelectivity("products", "category"))
                .isCloseTo(1.0 / 50, within(0.001));
    }

    @Test
    void estimateSelectivity_lowConfidence_returnsDefault() {
        ColumnStatistics cs = lowConfidence(1000, 100);
        collector.putStatistics("products", new PlannerTableStatistics(
                "products", Map.of("category", cs), Instant.now()));

        assertThat(estimator.estimateSelectivity("products", "category"))
                .isEqualTo(StatisticsBasedCostEstimator.DEFAULT_SELECTIVITY);
    }

    @Test
    void estimateSelectivity_noStatistics_returnsDefault() {
        assertThat(estimator.estimateSelectivity("missing", "col"))
                .isEqualTo(StatisticsBasedCostEstimator.DEFAULT_SELECTIVITY);
    }

    @Test
    void estimateSelectivity_unknownColumn_returnsDefault() {
        ColumnStatistics cs = confident(5000, 500);
        collector.putStatistics("t", new PlannerTableStatistics(
                "t", Map.of("a", cs), Instant.now()));

        assertThat(estimator.estimateSelectivity("t", "nonexistent"))
                .isEqualTo(StatisticsBasedCostEstimator.DEFAULT_SELECTIVITY);
    }

    // --- Index-derived statistics tests (R11.4) ---

    @Test
    void estimateRowCount_prefersIndexStats() {
        IndexStatisticsProvider indexProvider = table ->
                Map.of("col", confident(8000, 200));
        var indexEstimator = new StatisticsBasedCostEstimator(collector, indexProvider);

        // Even without connector stats, index stats should be used
        assertThat(indexEstimator.estimateRowCount("indexed_table"))
                .isCloseTo(8000.0, within(0.01));
    }

    @Test
    void estimateRowCount_indexStatsTakePrecedenceOverConnector() {
        IndexStatisticsProvider indexProvider = table ->
                Map.of("col", confident(3000, 100));
        var indexEstimator = new StatisticsBasedCostEstimator(collector, indexProvider);

        // Connector says 5000, index says 3000 — index wins
        collector.putStatistics("t", new PlannerTableStatistics(
                "t", Map.of("col", confident(5000, 500)), Instant.now()));

        assertThat(indexEstimator.estimateRowCount("t"))
                .isCloseTo(3000.0, within(0.01));
    }

    @Test
    void estimateSelectivity_prefersIndexNdv() {
        IndexStatisticsProvider indexProvider = table ->
                Map.of("category", confident(10000, 25));
        var indexEstimator = new StatisticsBasedCostEstimator(collector, indexProvider);

        assertThat(indexEstimator.estimateSelectivity("products", "category"))
                .isCloseTo(1.0 / 25, within(0.001));
    }

    @Test
    void estimateSelectivity_fallsBackWhenIndexHasNoColumn() {
        IndexStatisticsProvider indexProvider = table ->
                Map.of("other_col", confident(10000, 25));
        var indexEstimator = new StatisticsBasedCostEstimator(collector, indexProvider);

        // No index stats for "category", falls back to connector
        collector.putStatistics("products", new PlannerTableStatistics(
                "products", Map.of("category", confident(10000, 50)), Instant.now()));

        assertThat(indexEstimator.estimateSelectivity("products", "category"))
                .isCloseTo(1.0 / 50, within(0.001));
    }

    @Test
    void estimateRowCount_handlesIndexProviderException() {
        IndexStatisticsProvider failingProvider = table -> {
            throw new RuntimeException("index store unavailable");
        };
        var indexEstimator = new StatisticsBasedCostEstimator(collector, failingProvider);

        // Should fall back to default without throwing
        assertThat(indexEstimator.estimateRowCount("t"))
                .isEqualTo(StatisticsBasedCostEstimator.DEFAULT_ROW_COUNT);
    }

    @Test
    void estimateRowCount_nullProviderUsesConnectorStats() {
        var noIndexEstimator = new StatisticsBasedCostEstimator(collector, null);
        collector.putStatistics("t", new PlannerTableStatistics(
                "t", Map.of("col", confident(7000, 700)), Instant.now()));

        assertThat(noIndexEstimator.estimateRowCount("t"))
                .isCloseTo(7000.0, within(0.01));
    }
}
