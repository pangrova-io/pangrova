package org.pangrova.planner.stats;

import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.planner.cost.StatisticsBasedCostEstimator;
import org.pangrova.connector.spi.metadata.table.TableStatistics;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure SPI statistic-value conversion and CBO calibration with explicit inputs.
 * These cases do not prove a connector, SDK, deployment or SQL execution.
 *
 * <p><b>Validates: Requirements 1.50, 2.50</b></p>
 */
class ConnectorStatisticsIntegrationTest {

    /** Default row count used by StatisticsBasedCostEstimator when no stats available. */
    private static final double CBO_DEFAULT_ROW_COUNT = 1_000_000.0;

    @Test
    void cboProducesDifferentEstimatesForDifferentDistributions() {
        // Scenario 1: Small table (1000 rows, high NDV)
        StatisticsCollector.ConnectorStatisticsProvider smallTableProvider = tableName -> {
            if (new QualifiedTableName("catalog", "db", "small_orders").equals(tableName)) {
                return new TableStatistics(1_000L, 50_000L, -1, Map.of(
                        "id", new TableStatistics.ColumnStatistics(0, 1000, 1L, 1000L, 8.0),
                        "amount", new TableStatistics.ColumnStatistics(10, 500, 0L, 10000L, 8.0)
                ));
            }
            return TableStatistics.UNKNOWN;
        };

        StatisticsCollector smallCollector = new StatisticsCollector(smallTableProvider);
        StatisticsBasedCostEstimator smallEstimator = new StatisticsBasedCostEstimator(smallCollector);

        // Scenario 2: Large table (10M rows, low NDV on filter column)
        StatisticsCollector.ConnectorStatisticsProvider largeTableProvider = tableName -> {
            if (new QualifiedTableName("catalog", "db", "large_orders").equals(tableName)) {
                return new TableStatistics(10_000_000L, 5_000_000_000L, -1, Map.of(
                        "id", new TableStatistics.ColumnStatistics(0, 10_000_000, 1L, 10_000_000L, 8.0),
                        "status", new TableStatistics.ColumnStatistics(100, 5, 0L, 4L, 4.0)
                ));
            }
            return TableStatistics.UNKNOWN;
        };

        StatisticsCollector largeCollector = new StatisticsCollector(largeTableProvider);
        StatisticsBasedCostEstimator largeEstimator = new StatisticsBasedCostEstimator(largeCollector);

        // CBO should produce different row count estimates
        double smallRowCount = smallEstimator.estimateRowCount(new QualifiedTableName("catalog", "db", "small_orders"));
        double largeRowCount = largeEstimator.estimateRowCount(new QualifiedTableName("catalog", "db", "large_orders"));
        assertThat(smallRowCount).isEqualTo(1_000.0);
        assertThat(largeRowCount).isEqualTo(10_000_000.0);
        assertThat(largeRowCount).isGreaterThan(smallRowCount * 1000);

        // CBO should produce different selectivity estimates
        double smallSelectivity = smallEstimator.estimateSelectivity(new QualifiedTableName("catalog", "db", "small_orders"), "amount");
        double largeSelectivity = largeEstimator.estimateSelectivity(new QualifiedTableName("catalog", "db", "large_orders"), "status");
        // small: 1/500 = 0.002, large: 1/5 = 0.2
        assertThat(smallSelectivity).isLessThan(largeSelectivity);

        // CBO should produce different confidence-weighted cardinalities
        double smallCardinality = smallEstimator.confidenceWeightedCardinality(new QualifiedTableName("catalog", "db", "small_orders"));
        double largeCardinality = largeEstimator.confidenceWeightedCardinality(new QualifiedTableName("catalog", "db", "large_orders"));
        assertThat(largeCardinality).isGreaterThan(smallCardinality);
    }

    @Test
    void lazyLoadFromConnectorPopulatesStore() {
        StatisticsCollector.ConnectorStatisticsProvider provider = tableName -> {
            if (new QualifiedTableName("catalog", "schema", "orders").equals(tableName)) {
                return new TableStatistics(50_000L, 2_000_000L, -1, Map.of(
                        "id", new TableStatistics.ColumnStatistics(0, 50_000, 1L, 50_000L, 8.0),
                        "price", new TableStatistics.ColumnStatistics(100, 1_000, 0L, 99999L, 8.0)
                ));
            }
            return TableStatistics.UNKNOWN;
        };

        StatisticsCollector collector = new StatisticsCollector(provider);

        // Before any explicit analyzeTable call, getStatistics should lazy-load
        Optional<org.pangrova.planner.stats.PlannerTableStatistics> result =
                collector.getStatistics(new QualifiedTableName("catalog", "schema", "orders"));
        assertThat(result).isPresent();
        org.pangrova.planner.stats.PlannerTableStatistics stats = result.orElseThrow();
        assertThat(stats.tableName()).isEqualTo(new QualifiedTableName("catalog", "schema", "orders"));
        assertThat(stats.columns()).containsKeys("id", "price");

        // Verify column stats are converted from SPI correctly
        ColumnStatistics idStats = stats.columns().get("id");
        assertThat(idStats.rowCount()).isEqualTo(50_000L);
        assertThat(idStats.nullCount()).isEqualTo(0);
        assertThat(idStats.ndv()).isEqualTo(50_000);
        assertThat(idStats.confidenceScore()).isEqualTo(1.0);
    }

    @Test
    void unknownConnectorStatsFallsBackToDefault() {
        StatisticsCollector.ConnectorStatisticsProvider provider =
                tableName -> TableStatistics.UNKNOWN;

        StatisticsCollector collector = new StatisticsCollector(provider);
        StatisticsBasedCostEstimator estimator = new StatisticsBasedCostEstimator(collector);

        // Should fall back to default when connector returns UNKNOWN
        double rowCount = estimator.estimateRowCount(new QualifiedTableName("catalog", "db", "missing"));
        assertThat(rowCount).isEqualTo(CBO_DEFAULT_ROW_COUNT);
    }

    @Test
    void connectorProviderExceptionFallsBackGracefully() {
        StatisticsCollector.ConnectorStatisticsProvider failingProvider = tableName -> {
            throw new RuntimeException("Connection refused");
        };

        StatisticsCollector collector = new StatisticsCollector(failingProvider);
        StatisticsBasedCostEstimator estimator = new StatisticsBasedCostEstimator(collector);

        // Should not throw, falls back to default
        double rowCount = estimator.estimateRowCount(new QualifiedTableName("catalog", "db", "unreachable"));
        assertThat(rowCount).isEqualTo(CBO_DEFAULT_ROW_COUNT);
    }
}
