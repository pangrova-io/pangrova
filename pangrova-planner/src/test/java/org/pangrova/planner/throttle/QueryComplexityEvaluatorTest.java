package org.pangrova.planner.throttle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryComplexityEvaluatorTest {

    private final QueryComplexityEvaluator evaluator = new QueryComplexityEvaluator();

    @Test
    void allZeros_returnsZero() {
        var metrics = new PlanMetrics(0, 0, 0, 0, 0);
        ComplexityScore score = evaluator.evaluate(metrics);
        assertThat(score.value()).isEqualTo(0.0);
    }

    @Test
    void veryLargeValues_scoreStaysBelow1() {
        var metrics = new PlanMetrics(
                Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE,
                Integer.MAX_VALUE, Integer.MAX_VALUE);
        ComplexityScore score = evaluator.evaluate(metrics);
        assertThat(score.value()).isGreaterThan(0.0).isLessThanOrEqualTo(1.0);
    }

    @Test
    void moderateQuery_scoreInMiddleRange() {
        var metrics = new PlanMetrics(1_000_000, 100_000_000, 4, 3, 5);
        ComplexityScore score = evaluator.evaluate(metrics);
        // All metrics at sigmoid midpoint → each normalized to 0.5 → weighted sum = 0.5
        assertThat(score.value()).isCloseTo(0.5, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void smallQuery_lowScore() {
        var metrics = new PlanMetrics(100, 1000, 0, 0, 1);
        ComplexityScore score = evaluator.evaluate(metrics);
        assertThat(score.value()).isGreaterThanOrEqualTo(0.0).isLessThan(0.2);
    }

    @Test
    void heavyJoinQuery_higherScore() {
        var metrics = new PlanMetrics(10_000_000, 500_000_000, 10, 5, 8);
        ComplexityScore score = evaluator.evaluate(metrics);
        assertThat(score.value()).isGreaterThan(0.5).isLessThanOrEqualTo(1.0);
    }

    @Test
    void scoreIsMonotonicWithRows() {
        var low = new PlanMetrics(100, 0, 0, 0, 0);
        var high = new PlanMetrics(10_000_000, 0, 0, 0, 0);
        assertThat(evaluator.evaluate(high).value())
                .isGreaterThan(evaluator.evaluate(low).value());
    }
}
