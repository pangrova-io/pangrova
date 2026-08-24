package org.pangrova.planner.incremental;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link IncrementalAnalyzer} — analyzes DT defining queries
 * for incrementalizability and produces per-column merge strategies.
 */
class IncrementalAnalyzerTest {

    @Test
    void sumQuery_isIncrementalizable() {
        IncrementalAnalysis result = IncrementalAnalyzer.analyze(
                "SELECT category, SUM(amount) FROM orders GROUP BY category");

        assertThat(result.isIncrementalizable()).isTrue();
        assertThat(result.mergeStrategies()).hasSize(1);
        assertThat(result.mergeStrategies().get(0).mergeOperator()).isEqualTo(MergeOperator.SUM);
    }

    @Test
    void countQuery_mergesViaSUM() {
        IncrementalAnalysis result = IncrementalAnalyzer.analyze(
                "SELECT region, COUNT(*) FROM sales GROUP BY region");

        assertThat(result.isIncrementalizable()).isTrue();
        assertThat(result.mergeStrategies()).hasSize(1);
        assertThat(result.mergeStrategies().get(0).mergeOperator()).isEqualTo(MergeOperator.SUM);
    }

    @Test
    void minMaxQuery_isIncrementalizable() {
        IncrementalAnalysis result = IncrementalAnalyzer.analyze(
                "SELECT category, MIN(price), MAX(price) FROM products GROUP BY category");

        assertThat(result.isIncrementalizable()).isTrue();
        assertThat(result.mergeStrategies()).hasSize(2);
        assertThat(result.mergeStrategies().get(0).mergeOperator()).isEqualTo(MergeOperator.MIN);
        assertThat(result.mergeStrategies().get(1).mergeOperator()).isEqualTo(MergeOperator.MAX);
    }

    @Test
    void avgQuery_decomposesToSumCount() {
        IncrementalAnalysis result = IncrementalAnalyzer.analyze(
                "SELECT category, AVG(price) FROM products GROUP BY category");

        assertThat(result.isIncrementalizable()).isTrue();
        assertThat(result.mergeStrategies()).hasSize(1);
        assertThat(result.mergeStrategies().get(0).mergeOperator())
                .isEqualTo(MergeOperator.AVG_DECOMPOSE);
    }

    @Test
    void medianQuery_isNotIncrementalizable() {
        IncrementalAnalysis result = IncrementalAnalyzer.analyze(
                "SELECT category, MEDIAN(price) FROM products GROUP BY category");

        assertThat(result.isIncrementalizable()).isFalse();
        assertThat(result.reason()).contains("MEDIAN");
    }

    @Test
    void percentileQuery_isNotIncrementalizable() {
        IncrementalAnalysis result = IncrementalAnalyzer.analyze(
                "SELECT category, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) "
                        + "FROM products GROUP BY category");

        assertThat(result.isIncrementalizable()).isFalse();
        assertThat(result.reason()).contains("PERCENTILE_CONT");
    }

    @Test
    void noAggregation_isTriviallyIncrementalizable() {
        IncrementalAnalysis result = IncrementalAnalyzer.analyze(
                "SELECT * FROM orders WHERE status = 'ACTIVE'");

        assertThat(result.isIncrementalizable()).isTrue();
        assertThat(result.mergeStrategies()).isEmpty();
    }

    @Test
    void multipleAggregations_allIncrementalizable() {
        IncrementalAnalysis result = IncrementalAnalyzer.analyze(
                "SELECT category, SUM(amount), COUNT(*), MIN(price), MAX(price) "
                        + "FROM orders GROUP BY category");

        assertThat(result.isIncrementalizable()).isTrue();
        assertThat(result.mergeStrategies()).hasSize(4);
    }

    @Test
    void nullQuery_throwsNPE() {
        assertThatThrownBy(() -> IncrementalAnalyzer.analyze(null))
                .isInstanceOf(NullPointerException.class);
    }
}
