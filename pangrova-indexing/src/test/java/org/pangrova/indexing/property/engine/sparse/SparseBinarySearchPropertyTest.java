package org.pangrova.indexing.property.engine.sparse;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.indexing.engine.sparse.SparseStrategyProvider;
import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.predicate.RangeQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link SparseStrategyProvider} binary search evaluation.
 *
 * <p><b>Validates: Requirements 5.1, 5.2, 5.3</b></p>
 *
 * <p>Core property: for any Sparse index built from sorted data and any
 * RangeQuery overlapping the data range, evaluate returns KEEP (no false negatives).</p>
 */
class SparseBinarySearchPropertyTest {

    private final SparseStrategyProvider provider = new SparseStrategyProvider();

    // ---------------------------------------------------------------
    // Property 1: No false negatives — overlapping range always KEEP
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void overlappingRangeNeverReturnsSkip(
            @ForAll("sortedIntegerValues") List<Integer> values,
            @ForAll @IntRange(min = 0, max = 1) int inclusiveFlag) {

        if (values.isEmpty()) return;

        IndexData indexData = buildSparseIndex(values, 1);

        int dataMin = values.get(0);
        int dataMax = values.get(values.size() - 1);

        // Pick a range that overlaps at least one value:
        // lower = dataMin - 1, upper = dataMax + 1 (covers entire data range)
        boolean inclusive = inclusiveFlag == 1;
        RangeQuery range = new RangeQuery("col", dataMin - 1, dataMax + 1, true, true);

        IndexEvalResult result = provider.evaluate(indexData, range);
        assertThat(result)
                .as("Range [%d, %d] overlaps data [%d, %d] — must not SKIP",
                        dataMin - 1, dataMax + 1, dataMin, dataMax)
                .isNotEqualTo(IndexEvalResult.SKIP);
    }

    @Property(tries = 10)
    void rangeOverlappingMiddleValueReturnsKeep(
            @ForAll("sortedIntegerValues") List<Integer> values) {

        if (values.size() < 2) return;

        IndexData indexData = buildSparseIndex(values, 1);

        // Pick a value in the middle and create a tight range around it
        int midIdx = values.size() / 2;
        int midVal = values.get(midIdx);
        RangeQuery range = new RangeQuery("col", midVal, midVal, true, true);

        IndexEvalResult result = provider.evaluate(indexData, range);
        assertThat(result)
                .as("Range [%d, %d] contains exact data value — must be KEEP", midVal, midVal)
                .isEqualTo(IndexEvalResult.KEEP);
    }

    // ---------------------------------------------------------------
    // Property 2: Range completely outside data → SKIP
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void rangeCompletelyBelowDataReturnsSkip(
            @ForAll @IntRange(min = 5, max = 100) int valueCount) {

        List<Integer> values = generateSortedValues(valueCount, 1000, 5000);
        IndexData indexData = buildSparseIndex(values, 1);

        // Range entirely below the minimum value
        RangeQuery range = new RangeQuery("col", 0, 500, true, true);

        IndexEvalResult result = provider.evaluate(indexData, range);
        assertThat(result)
                .as("Range [0, 500] is entirely below data min=%d — should SKIP", values.get(0))
                .isEqualTo(IndexEvalResult.SKIP);
    }

    @Property(tries = 10)
    void rangeCompletelyAboveDataReturnsSkip(
            @ForAll @IntRange(min = 5, max = 100) int valueCount) {

        List<Integer> values = generateSortedValues(valueCount, 100, 500);
        IndexData indexData = buildSparseIndex(values, 1);

        int dataMax = values.get(values.size() - 1);
        // Range entirely above the maximum value
        RangeQuery range = new RangeQuery("col", dataMax + 100, dataMax + 200, true, true);

        IndexEvalResult result = provider.evaluate(indexData, range);
        assertThat(result)
                .as("Range [%d, %d] is entirely above data max=%d — should SKIP",
                        dataMax + 100, dataMax + 200, dataMax)
                .isEqualTo(IndexEvalResult.SKIP);
    }

    // ---------------------------------------------------------------
    // Property 3: Open-ended ranges that overlap → KEEP
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void openEndedLowerBoundOverlappingReturnsKeep(
            @ForAll("sortedIntegerValues") List<Integer> values) {

        if (values.isEmpty()) return;

        IndexData indexData = buildSparseIndex(values, 1);

        int dataMax = values.get(values.size() - 1);
        // upper=null means open-ended upper bound: range is [lower, +∞)
        // lower is below or at data max, so it overlaps
        RangeQuery range = new RangeQuery("col", null, dataMax, true, true);

        IndexEvalResult result = provider.evaluate(indexData, range);
        assertThat(result)
                .as("Open-ended range (null, %d] overlaps data — must not SKIP", dataMax)
                .isNotEqualTo(IndexEvalResult.SKIP);
    }

    @Property(tries = 10)
    void openEndedUpperBoundOverlappingReturnsKeep(
            @ForAll("sortedIntegerValues") List<Integer> values) {

        if (values.isEmpty()) return;

        IndexData indexData = buildSparseIndex(values, 1);

        int dataMin = values.get(0);
        // lower=null means open-ended lower bound: range is (-∞, upper]
        // upper is at or above data min, so it overlaps
        RangeQuery range = new RangeQuery("col", dataMin, null, true, true);

        IndexEvalResult result = provider.evaluate(indexData, range);
        assertThat(result)
                .as("Open-ended range [%d, null) overlaps data — must not SKIP", dataMin)
                .isNotEqualTo(IndexEvalResult.SKIP);
    }

    // ---------------------------------------------------------------
    // Property 4: Single-value range matching exact entry → KEEP
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void singleValueRangeMatchingExactEntryReturnsKeep(
            @ForAll @IntRange(min = 1, max = 200) int valueCount,
            @ForAll @IntRange(min = 0, max = 199) int targetIdx) {

        if (targetIdx >= valueCount) return;

        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < valueCount; i++) {
            values.add(i * 10);
        }
        IndexData indexData = buildSparseIndex(values, 1);

        int targetKey = values.get(targetIdx);
        RangeQuery range = new RangeQuery("col", targetKey, targetKey, true, true);

        IndexEvalResult result = provider.evaluate(indexData, range);
        assertThat(result)
                .as("Single-value range [%d, %d] matches exact entry — must be KEEP",
                        targetKey, targetKey)
                .isEqualTo(IndexEvalResult.KEEP);
    }

    // ---------------------------------------------------------------
    // Build-then-evaluate round-trip with small stride
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void buildThenEvaluateWithSmallStridePreservesNoFalseNegatives(
            @ForAll @IntRange(min = 10, max = 200) int rowCount) {

        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            rows.add(List.of(i));
        }
        IndexData indexData = provider.build(rows.iterator(), Map.of("sparse.stride", "1"));

        // Range covering the middle portion — guaranteed to overlap
        int lo = rowCount / 4;
        int hi = rowCount * 3 / 4;
        RangeQuery range = new RangeQuery("col", lo, hi, true, true);

        IndexEvalResult result = provider.evaluate(indexData, range);
        assertThat(result)
                .as("Range [%d, %d] overlaps data [0, %d] — must be KEEP", lo, hi, rowCount - 1)
                .isEqualTo(IndexEvalResult.KEEP);
    }

    // ---------------------------------------------------------------
    // Generators and helpers
    // ---------------------------------------------------------------

    @Provide
    Arbitrary<List<Integer>> sortedIntegerValues() {
        return Combinators.combine(
                Arbitraries.integers().between(1, 200),   // count
                Arbitraries.integers().between(0, 10000)   // base offset
        ).as((count, base) -> {
            List<Integer> values = new ArrayList<>();
            int current = base;
            for (int i = 0; i < count; i++) {
                values.add(current);
                current += 1 + (i % 5); // varying gaps to avoid uniform spacing
            }
            return values;
        });
    }

    /**
     * Build a Sparse index from sorted integer values with the given stride.
     * Each value becomes a single-element row.
     */
    private IndexData buildSparseIndex(List<Integer> sortedValues, int stride) {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int v : sortedValues) {
            rows.add(List.of(v));
        }
        return provider.build(rows.iterator(), Map.of("sparse.stride", String.valueOf(stride)));
    }

    /**
     * Generate a sorted list of distinct integer values in [rangeMin, rangeMax].
     */
    private static List<Integer> generateSortedValues(int count, int rangeMin, int rangeMax) {
        List<Integer> values = new ArrayList<>();
        int step = Math.max(1, (rangeMax - rangeMin) / count);
        for (int i = 0; i < count; i++) {
            values.add(rangeMin + i * step);
        }
        Collections.sort(values);
        return values;
    }
}
