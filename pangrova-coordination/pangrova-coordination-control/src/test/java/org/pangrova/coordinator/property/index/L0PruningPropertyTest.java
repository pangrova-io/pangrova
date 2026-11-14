package org.pangrova.coordinator.property.index;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Combinators;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;
import org.pangrova.connector.api.enums.ColumnComparisonOperator;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test: L0 pruning never produces false negatives.
 *
 * <p>If a file's min/max range contains a value that satisfies the predicate,
 * L0 must NOT prune the file (noRowsSatisfy must return false).</p>
 */
class L0PruningPropertyTest {

    @Property(tries = 10)
    void equalTo_valueInRange_neverPruned(
            @ForAll("rangeWithContainedValue") RangeAndValue rv) {
        FileColumnStatistics stats = new FileColumnStatistics(
                "col", rv.min, rv.max, 0, 100, 10, 1024);
        // Value is within [min, max], so noRowsSatisfy(EQUAL, value) must be false
        assertThat(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, rv.contained))
                .as("L0 must not prune when value %s is in [%s, %s]",
                        rv.contained, rv.min, rv.max)
                .isFalse();
    }

    @Property(tries = 10)
    void equalTo_valueOutOfRange_pruned(
            @ForAll("rangeWithExcludedValue") RangeAndValue rv) {
        FileColumnStatistics stats = new FileColumnStatistics(
                "col", rv.min, rv.max, 0, 100, 10, 1024);
        // Value is outside [min, max], so noRowsSatisfy(EQUAL, value) must be true
        assertThat(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, rv.contained))
                .as("L0 should prune when value %s is outside [%s, %s]",
                        rv.contained, rv.min, rv.max)
                .isTrue();
    }

    @Property(tries = 10)
    void rangeQuery_overlapping_neverPruned(
            @ForAll("overlappingRange") OverlappingRange or) {
        FileColumnStatistics stats = new FileColumnStatistics(
                "col", or.fileMin, or.fileMax, 0, 100, 10, 1024);
        // Range [queryLower, queryUpper] overlaps [fileMin, fileMax]
        // So neither bound should cause pruning
        if (or.queryLower != null) {
            boolean prunedByLower = stats.noRowsSatisfy(ColumnComparisonOperator.GREATER_THAN_OR_EQUAL, or.queryLower);
            assertThat(prunedByLower)
                    .as("L0 must not prune overlapping range: file=[%s,%s] query>=%s",
                            or.fileMin, or.fileMax, or.queryLower)
                    .isFalse();
        }
        if (or.queryUpper != null) {
            boolean prunedByUpper = stats.noRowsSatisfy(ColumnComparisonOperator.LESS_THAN_OR_EQUAL, or.queryUpper);
            assertThat(prunedByUpper)
                    .as("L0 must not prune overlapping range: file=[%s,%s] query<=%s",
                            or.fileMin, or.fileMax, or.queryUpper)
                    .isFalse();
        }
    }

    @Property(tries = 10)
    void inList_anyValueInRange_neverPruned(
            @ForAll("inListWithMatch") InListAndStats ils) {
        FileColumnStatistics stats = new FileColumnStatistics(
                "col", ils.min, ils.max, 0, 100, 10, 1024);
        // At least one value is in range, so not all values are out of range
        boolean allOut = true;
        for (Comparable<?> val : ils.values) {
            if (!stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, val)) {
                allOut = false;
                break;
            }
        }
        assertThat(allOut)
                .as("L0 must not prune InList when at least one value is in [%s, %s]",
                        ils.min, ils.max)
                .isFalse();
    }

    @Property(tries = 10)
    void nullStats_neverPrunes() {
        // null min/max → conservative, never prune
        FileColumnStatistics stats = new FileColumnStatistics(
                "col", null, null, 0, 100, 0, 1024);
        assertThat(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 42L)).isFalse();
        assertThat(stats.noRowsSatisfy(ColumnComparisonOperator.LESS_THAN, 42L)).isFalse();
        assertThat(stats.noRowsSatisfy(ColumnComparisonOperator.GREATER_THAN, 42L)).isFalse();
    }

    // --- Arbitraries ---

    @Provide
    Arbitrary<RangeAndValue> rangeWithContainedValue() {
        return Arbitraries.longs().between(-10_000, 10_000).flatMap(min ->
                Arbitraries.longs().between(min, min + 1000).flatMap(max ->
                        Arbitraries.longs().between(min, max).map(val ->
                                new RangeAndValue(min, max, val))));
    }

    @Provide
    Arbitrary<RangeAndValue> rangeWithExcludedValue() {
        return Arbitraries.longs().between(-10_000, 10_000).flatMap(min ->
                Arbitraries.longs().between(min + 1, min + 1000).flatMap(max ->
                        Arbitraries.oneOf(
                                Arbitraries.longs().between(min - 500, min - 1),
                                Arbitraries.longs().between(max + 1, max + 500)
                        ).map(val -> new RangeAndValue(min, max, val))));
    }

    @Provide
    Arbitrary<OverlappingRange> overlappingRange() {
        return Arbitraries.longs().between(-10_000, 10_000).flatMap(fileMin ->
                Arbitraries.longs().between(fileMin, fileMin + 1000).flatMap(fileMax ->
                        // Query range overlaps file range
                        Arbitraries.longs().between(fileMin - 500, fileMax).flatMap(qLower ->
                                Arbitraries.longs().between(Math.max(qLower, fileMin), fileMax + 500)
                                        .map(qUpper -> new OverlappingRange(fileMin, fileMax, qLower, qUpper)))));
    }

    @Provide
    Arbitrary<InListAndStats> inListWithMatch() {
        return Arbitraries.longs().between(-10_000, 10_000).flatMap(min ->
                Arbitraries.longs().between(min, min + 1000).flatMap(max ->
                        Arbitraries.longs().between(min, max).flatMap(inRangeVal ->
                                Arbitraries.longs().between(-20_000, 20_000)
                                        .list().ofMinSize(0).ofMaxSize(5)
                                        .map(others -> {
                                            List<Comparable<?>> vals = new java.util.ArrayList<>(others);
                                            vals.add(inRangeVal);
                                            return new InListAndStats(min, max, vals);
                                        }))));
    }

    record RangeAndValue(Long min, Long max, Long contained) {}
    record OverlappingRange(Long fileMin, Long fileMax, Long queryLower, Long queryUpper) {}
    record InListAndStats(Long min, Long max, List<Comparable<?>> values) {}
}
