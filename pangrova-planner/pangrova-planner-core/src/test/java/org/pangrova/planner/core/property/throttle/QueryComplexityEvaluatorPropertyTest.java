package org.pangrova.planner.core.property.throttle;

import org.pangrova.planner.core.throttle.ComplexityScore;
import org.pangrova.planner.core.throttle.PlanMetrics;
import org.pangrova.planner.core.throttle.QueryComplexityEvaluator;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Combinators;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Feature: sql-planning, Property 10: Complexity score bounded
 *
 * For any ExecutionPlan with non-negative estimated rows, bytes, join count,
 * subquery depth, and aggregation count, the QueryComplexityEvaluator SHALL
 * return a ComplexityScore with value in [0.0, 1.0].
 *
 * Validates: Requirements 6.6
 */
class QueryComplexityEvaluatorPropertyTest {

    private final QueryComplexityEvaluator evaluator = new QueryComplexityEvaluator();

    // Property 10: Complexity score bounded
    @Property(tries = 10)
    void complexityScoreAlwaysBounded(@ForAll("planMetrics") PlanMetrics metrics) {
        ComplexityScore score = evaluator.evaluate(metrics);
        assertThat(score.value())
                .as("score for %s", metrics)
                .isGreaterThanOrEqualTo(0.0)
                .isLessThanOrEqualTo(1.0);
    }

    @Provide
    Arbitrary<PlanMetrics> planMetrics() {
        Arbitrary<Long> rows = Arbitraries.longs().between(0, Long.MAX_VALUE);
        Arbitrary<Long> bytes = Arbitraries.longs().between(0, Long.MAX_VALUE);
        Arbitrary<Integer> joins = Arbitraries.integers().between(0, 100);
        Arbitrary<Integer> depth = Arbitraries.integers().between(0, 50);
        Arbitrary<Integer> aggs = Arbitraries.integers().between(0, 100);
        return Combinators.combine(rows, bytes, joins, depth, aggs)
                .as(PlanMetrics::new);
    }
}
