package org.pangrova.planner.statistics;

import org.pangrova.planner.api.statistics.ColumnStatistics;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StatisticsCollectorTest {

    private StatisticsCollector collector;

    @BeforeEach
    void setUp() {
        collector = new StatisticsCollector();
    }

    @Test
    void analyzeTableProducesValidStatistics() {
        PlannerTableStatistics stats = collector.analyzeTable(new QualifiedTableName("test", "db", "orders"), List.of("id", "amount"));

        assertThat(stats.tableName()).isEqualTo(new QualifiedTableName("test", "db", "orders"));
        assertThat(stats.columns()).containsKeys("id", "amount");
        stats.columns().values().forEach(col -> {
            assertThat(col.rowCount()).isPositive();
            assertThat(col.confidenceScore()).isBetween(0.1, 1.0);
        });
    }

    @Test
    void confidenceScoreInBounds() {
        assertThat(collector.computeConfidence(Duration.ZERO, 0.5)).isBetween(0.1, 1.0);
        assertThat(collector.computeConfidence(Duration.ofHours(100), 1.0)).isEqualTo(0.1);
        assertThat(collector.computeConfidence(Duration.ofMinutes(1), 0.001)).isBetween(0.1, 1.0);
    }

    @Test
    void confidenceDecreasesWithStaleness() {
        double fresh = collector.computeConfidence(Duration.ofHours(1), 0.1);
        double stale = collector.computeConfidence(Duration.ofHours(5), 0.1);

        assertThat(fresh).isGreaterThanOrEqualTo(stale);
    }

    @Test
    void histogramTriggeredForLowCardinality() {
        // NDV=5, rowCount=10000 → 5 < 10000*0.01=100 → true
        assertThat(collector.shouldCollectHistogram("status", 5, 10_000, false, false))
                .isTrue();
    }

    @Test
    void histogramTriggeredForJoinKey() {
        assertThat(collector.shouldCollectHistogram("user_id", 50_000, 100_000, true, false))
                .isTrue();
    }

    @Test
    void histogramSkippedForHighCardinalityNonKey() {
        // NDV=5000, rowCount=10000 → 5000 >= 100 → false
        assertThat(collector.shouldCollectHistogram("uuid", 5000, 10_000, false, false))
                .isFalse();
    }

    @Test
    void stalenessDetectedWhenThresholdExceeded() {
        collector.analyzeTable(new QualifiedTableName("test", "db", "t"), List.of("a"));
        Instant future = Instant.now().plus(Duration.ofHours(25));

        assertThat(collector.checkStaleness(new QualifiedTableName("test", "db", "t"), future, Duration.ofHours(24)))
                .isTrue();
    }

    @Test
    void stalenessNotDetectedWhenFresh() {
        collector.analyzeTable(new QualifiedTableName("test", "db", "t"), List.of("a"));
        Instant soon = Instant.now().plus(Duration.ofMinutes(5));

        assertThat(collector.checkStaleness(new QualifiedTableName("test", "db", "t"), soon, Duration.ofHours(24)))
                .isFalse();
    }

    @Test
    void stalenessReturnsTrueForUnknownTable() {
        assertThat(collector.checkStaleness(new QualifiedTableName("test", "db", "unknown"), Instant.now(), Duration.ofHours(1)))
                .isTrue();
    }

    @Test
    void incrementalCollectionUpdatesStore() {
        collector.analyzeTable(new QualifiedTableName("test", "db", "t"), List.of("col1"));
        PlannerTableStatistics updated = collector.collectIncremental(new QualifiedTableName("test", "db", "t"), 500);

        assertThat(updated.tableName()).isEqualTo(new QualifiedTableName("test", "db", "t"));
        assertThat(collector.getStatistics(new QualifiedTableName("test", "db", "t"))).isPresent();
    }

    @Test
    void readFileFooterStatsReturnsFullConfidence() {
        ColumnStatistics footer = collector.readFileFooterStats("price", 1000, 10, 0L, 999L);

        assertThat(footer.confidenceScore()).isEqualTo(1.0);
        assertThat(footer.rowCount()).isEqualTo(1000);
        assertThat(footer.nullCount()).isEqualTo(10);
    }
}
