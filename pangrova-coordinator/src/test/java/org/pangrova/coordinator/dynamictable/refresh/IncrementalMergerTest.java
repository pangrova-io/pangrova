package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.planner.incremental.ColumnMergeStrategy;
import org.pangrova.planner.incremental.MergeOperator;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link IncrementalMerger} — merges old (materialized) aggregation
 * results with delta (incremental) results using operator-specific strategies.
 *
 * <p>Verifies that incremental merge produces the same result as full recompute
 * for SUM, MIN, MAX, and AVG_DECOMPOSE operators.</p>
 */
class IncrementalMergerTest {

    @Test
    void sumMerge_producesCorrectResult() {
        // Old: category=A, SUM_amount=100
        IncrementalResult old = new IncrementalResult(
                List.of(Map.of("category", "A", "SUM_amount", 100L)),
                50, 0);
        // Delta: category=A, SUM_amount=50
        IncrementalResult delta = new IncrementalResult(
                List.of(Map.of("category", "A", "SUM_amount", 50L)),
                10, 0);

        List<ColumnMergeStrategy> strategies = List.of(
                new ColumnMergeStrategy("SUM", MergeOperator.SUM));

        IncrementalResult merged = IncrementalMerger.merge(old, delta, strategies,
                List.of("category"));

        assertThat(merged.records()).hasSize(1);
        assertThat(merged.records().get(0).get("SUM_amount")).isEqualTo(150L);
        assertThat(merged.records().get(0).get("category")).isEqualTo("A");
    }

    @Test
    void minMerge_selectsSmaller() {
        IncrementalResult old = new IncrementalResult(
                List.of(Map.of("category", "A", "MIN_price", 10L)),
                50, 0);
        IncrementalResult delta = new IncrementalResult(
                List.of(Map.of("category", "A", "MIN_price", 5L)),
                10, 0);

        List<ColumnMergeStrategy> strategies = List.of(
                new ColumnMergeStrategy("MIN", MergeOperator.MIN));

        IncrementalResult merged = IncrementalMerger.merge(old, delta, strategies,
                List.of("category"));

        assertThat(merged.records()).hasSize(1);
        assertThat(merged.records().get(0).get("MIN_price")).isEqualTo(5L);
    }

    @Test
    void maxMerge_selectsLarger() {
        IncrementalResult old = new IncrementalResult(
                List.of(Map.of("category", "A", "MAX_price", 10L)),
                50, 0);
        IncrementalResult delta = new IncrementalResult(
                List.of(Map.of("category", "A", "MAX_price", 25L)),
                10, 0);

        List<ColumnMergeStrategy> strategies = List.of(
                new ColumnMergeStrategy("MAX", MergeOperator.MAX));

        IncrementalResult merged = IncrementalMerger.merge(old, delta, strategies,
                List.of("category"));

        assertThat(merged.records()).hasSize(1);
        assertThat(merged.records().get(0).get("MAX_price")).isEqualTo(25L);
    }

    @Test
    void newGroupInDelta_addedDirectly() {
        IncrementalResult old = new IncrementalResult(
                List.of(Map.of("category", "A", "SUM_amount", 100L)),
                50, 0);
        IncrementalResult delta = new IncrementalResult(
                List.of(Map.of("category", "B", "SUM_amount", 75L)),
                10, 0);

        List<ColumnMergeStrategy> strategies = List.of(
                new ColumnMergeStrategy("SUM", MergeOperator.SUM));

        IncrementalResult merged = IncrementalMerger.merge(old, delta, strategies,
                List.of("category"));

        assertThat(merged.records()).hasSize(2);
        Map<String, Object> groupA = findByCategory(merged, "A");
        Map<String, Object> groupB = findByCategory(merged, "B");
        assertThat(groupA.get("SUM_amount")).isEqualTo(100L);
        assertThat(groupB.get("SUM_amount")).isEqualTo(75L);
    }

    @Test
    void unchangedOldGroup_preserved() {
        IncrementalResult old = new IncrementalResult(
                List.of(
                        Map.of("category", "A", "SUM_amount", 100L),
                        Map.of("category", "B", "SUM_amount", 200L)),
                100, 0);
        // Delta only has category A
        IncrementalResult delta = new IncrementalResult(
                List.of(Map.of("category", "A", "SUM_amount", 50L)),
                10, 0);

        List<ColumnMergeStrategy> strategies = List.of(
                new ColumnMergeStrategy("SUM", MergeOperator.SUM));

        IncrementalResult merged = IncrementalMerger.merge(old, delta, strategies,
                List.of("category"));

        assertThat(merged.records()).hasSize(2);
        Map<String, Object> groupB = findByCategory(merged, "B");
        assertThat(groupB.get("SUM_amount")).isEqualTo(200L);
    }

    @Test
    void emptyOldResult_deltaBecomesResult() {
        IncrementalResult old = IncrementalResult.empty();
        IncrementalResult delta = new IncrementalResult(
                List.of(Map.of("category", "A", "SUM_amount", 50L)),
                10, 0);

        List<ColumnMergeStrategy> strategies = List.of(
                new ColumnMergeStrategy("SUM", MergeOperator.SUM));

        IncrementalResult merged = IncrementalMerger.merge(old, delta, strategies,
                List.of("category"));

        assertThat(merged.records()).hasSize(1);
        assertThat(merged.records().get(0).get("SUM_amount")).isEqualTo(50L);
    }

    @Test
    void sumMerge_matchesFullRecompute() {
        // Simulate: old has partial aggregation, delta has new data
        // Full recompute would be: A=100+50=150, B=200+75=275, C=30 (new)
        IncrementalResult old = new IncrementalResult(
                List.of(
                        Map.of("category", "A", "SUM_amount", 100L),
                        Map.of("category", "B", "SUM_amount", 200L)),
                100, 0);
        IncrementalResult delta = new IncrementalResult(
                List.of(
                        Map.of("category", "A", "SUM_amount", 50L),
                        Map.of("category", "B", "SUM_amount", 75L),
                        Map.of("category", "C", "SUM_amount", 30L)),
                30, 0);

        List<ColumnMergeStrategy> strategies = List.of(
                new ColumnMergeStrategy("SUM", MergeOperator.SUM));

        IncrementalResult merged = IncrementalMerger.merge(old, delta, strategies,
                List.of("category"));

        assertThat(merged.records()).hasSize(3);
        assertThat(findByCategory(merged, "A").get("SUM_amount")).isEqualTo(150L);
        assertThat(findByCategory(merged, "B").get("SUM_amount")).isEqualTo(275L);
        assertThat(findByCategory(merged, "C").get("SUM_amount")).isEqualTo(30L);
        assertThat(merged.processedRows()).isEqualTo(130L);
    }

    @Test
    void nullInputs_throwNPE() {
        IncrementalResult result = IncrementalResult.empty();
        List<ColumnMergeStrategy> strategies = List.of();
        List<String> keys = List.of();

        assertThatThrownBy(() -> IncrementalMerger.merge(null, result, strategies, keys))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> IncrementalMerger.merge(result, null, strategies, keys))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void avgDecompose_computesAverageNotSum() {
        // Simulate AVG(10, 20, 30) decomposed as sum + count:
        // Old: sum=30 (10+20), count=2
        // Delta: sum=30, count=1
        // Expected AVG = (30+30) / (2+1) = 20, NOT 60
        IncrementalResult old = new IncrementalResult(
                List.of(Map.of("category", "A", "AVG_amount", 30.0, "COUNT_star", 2L)),
                2, 0);
        IncrementalResult delta = new IncrementalResult(
                List.of(Map.of("category", "A", "AVG_amount", 30.0, "COUNT_star", 1L)),
                1, 0);

        List<ColumnMergeStrategy> strategies = List.of(
                new ColumnMergeStrategy("AVG", MergeOperator.AVG_DECOMPOSE),
                new ColumnMergeStrategy("COUNT", MergeOperator.SUM));

        IncrementalResult merged = IncrementalMerger.merge(old, delta, strategies,
                List.of("category"));

        assertThat(merged.records()).hasSize(1);
        // AVG(10, 20, 30) = 20, not 60
        assertThat(((Number) merged.records().get(0).get("AVG_amount")).doubleValue())
                .isEqualTo(20.0);
        // Count should be merged as sum: 2 + 1 = 3
        assertThat(merged.records().get(0).get("COUNT_star")).isEqualTo(3L);
    }

    private static Map<String, Object> findByCategory(IncrementalResult result, String category) {
        return result.records().stream()
                .filter(r -> category.equals(r.get("category")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Category not found: " + category));
    }
}
