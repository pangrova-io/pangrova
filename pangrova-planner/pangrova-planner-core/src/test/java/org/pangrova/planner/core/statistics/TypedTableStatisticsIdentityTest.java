package org.pangrova.planner.core.statistics;

import org.pangrova.planner.api.statistics.ColumnStatistics;

import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.cost.StatisticsBasedCostEstimator;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure typed-cache and feedback calculations; not connector or execution evidence. */
class TypedTableStatisticsIdentityTest {
    private static final QualifiedTableName FIRST = new QualifiedTableName("catalog", "a.b", "rows");
    private static final QualifiedTableName SECOND = new QualifiedTableName("catalog", "a", "b.rows");

    @Test
    void displayCollisionDoesNotShareStatisticsOrFeedback() {
        var collector = new StatisticsCollector();
        collector.putStatistics(FIRST, snapshot(FIRST, 2));
        collector.putStatistics(SECOND, snapshot(SECOND, 7));
        var feedback = new CardinalityFeedbackStore();
        var estimator = new StatisticsBasedCostEstimator(collector, null, null, feedback);
        assertThat(FIRST.toString()).isEqualTo(SECOND.toString());
        assertThat(estimator.estimateRowCount(FIRST)).isEqualTo(2);
        assertThat(estimator.estimateRowCount(SECOND)).isEqualTo(7);
        feedback.record(FIRST.canonicalKey(), 2, 10);
        assertThat(estimator.estimateRowCount(FIRST)).isEqualTo(10);
        assertThat(estimator.estimateRowCount(SECOND)).isEqualTo(7);
        collector.invalidate(FIRST);
        assertThat(collector.getStatistics(FIRST)).isEmpty();
        assertThat(collector.getStatistics(SECOND).orElseThrow().tableName()).isEqualTo(SECOND);
    }

    @Test
    void rejectedIdentityMismatchDoesNotOverwriteAnExistingEntry() {
        var collector = new StatisticsCollector();
        var original = snapshot(FIRST, 2);
        collector.putStatistics(FIRST, original);
        assertThatThrownBy(() -> collector.putStatistics(FIRST, snapshot(SECOND, 7)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("identity");
        assertThat(collector.getStatistics(FIRST)).containsSame(original);
        assertThat(collector.getStatistics(SECOND)).isEmpty();
    }

    private static PlannerTableStatistics snapshot(QualifiedTableName table, long rows) {
        return new PlannerTableStatistics(table, Map.of("id", new ColumnStatistics(
                rows, 0, null, null, rows, 8, Optional.empty(), 1)), Instant.EPOCH);
    }
}
