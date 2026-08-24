package org.pangrova.planner.rule;

import org.pangrova.planner.stats.ColumnStatistics;
import org.pangrova.planner.stats.StatisticsCollector;
import org.pangrova.planner.stats.PlannerTableStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionPruningRuleTest {

    private StatisticsCollector collector;
    private PartitionPruningRule rule;

    @BeforeEach
    void setUp() {
        collector = new StatisticsCollector();
        rule = new PartitionPruningRule();
        putColumnStats("orders", "date", 0L, 100L);
    }

    @Test
    void prunes_whenPredicateBeforeMinRange() {
        assertThat(rule.shouldPrune("orders", "date", -5L, collector))
                .isTrue();
    }

    @Test
    void prunes_whenPredicateAfterMaxRange() {
        assertThat(rule.shouldPrune("orders", "date", 200L, collector))
                .isTrue();
    }

    @Test
    void keeps_whenPredicateWithinRange() {
        assertThat(rule.shouldPrune("orders", "date", 50L, collector))
                .isFalse();
    }

    @Test
    void keeps_whenPredicateEqualsMin() {
        assertThat(rule.shouldPrune("orders", "date", 0L, collector))
                .isFalse();
    }

    @Test
    void keeps_whenPredicateEqualsMax() {
        assertThat(rule.shouldPrune("orders", "date", 100L, collector))
                .isFalse();
    }

    @Test
    void keeps_whenNoStatisticsAvailable() {
        assertThat(rule.shouldPrune("unknown", "col", 50L, collector))
                .isFalse();
    }

    @Test
    void keeps_whenColumnNotInStatistics() {
        assertThat(rule.shouldPrune("orders", "missing_col", 50L, collector))
                .isFalse();
    }

    private void putColumnStats(String table, String column,
                                Comparable<?> min, Comparable<?> max) {
        ColumnStatistics cs = new ColumnStatistics(
                1000L, 0L, min, max, 100L, 8.0, Optional.empty(), 1.0);
        collector.putStatistics(table,
                new PlannerTableStatistics(table, Map.of(column, cs), Instant.now()));
    }
}
