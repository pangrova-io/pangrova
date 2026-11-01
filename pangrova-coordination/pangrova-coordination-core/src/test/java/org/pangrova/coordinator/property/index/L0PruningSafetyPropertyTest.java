package org.pangrova.coordinator.property.index;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.spi.connector.index.statistics.FileColumnStatistics;
import org.pangrova.spi.connector.index.statistics.FileColumnStatistics.ComparisonOp;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property P1: L0 Pruning Safety (No False Negatives).
 *
 * <p>∀ random file statistics and predicates, if {@code noRowsSatisfy()} returns true,
 * then no row within [min, max] satisfies the predicate.</p>
 *
 * <p>Validates: Requirements 2.AC5</p>
 *
 * <p>Strategy: for each ComparisonOp, generate random [min, max] ranges and a random
 * witness value within that range. If {@code noRowsSatisfy(op, queryValue)} returns true,
 * then the witness must NOT satisfy the predicate — proving no false negatives.</p>
 */
class L0PruningSafetyPropertyTest {

    // ── EqualTo safety ──────────────────────────────────────────────────

    /**
     * If noRowsSatisfy(EQUAL, queryValue) is true, then no value in [min, max]
     * equals queryValue. We verify by checking the queryValue itself is outside [min, max].
     */
    @Property(tries = 10)
    void equalPruneSafety_noWitnessInRangeSatisfies(
            @ForAll("statsAndQuery") StatsAndQuery sq) {
        boolean pruned = sq.stats.noRowsSatisfy(ComparisonOp.EQUAL, sq.queryValue);
        if (pruned) {
            // Safety: queryValue must be outside [min, max]
            @SuppressWarnings("unchecked")
            Comparable<Object> q = (Comparable<Object>) (Comparable<?>) sq.queryValue;
            @SuppressWarnings("unchecked")
            Comparable<Object> min = (Comparable<Object>) sq.stats.minValue();
            @SuppressWarnings("unchecked")
            Comparable<Object> max = (Comparable<Object>) sq.stats.maxValue();
            assertThat(q.compareTo(min) < 0 || q.compareTo(max) > 0)
                    .as("EQUAL prune claimed safe but query %s is within [%s, %s]",
                            sq.queryValue, sq.stats.minValue(), sq.stats.maxValue())
                    .isTrue();
        }
    }

    // ── LESS_THAN safety ────────────────────────────────────────────────

    /**
     * If noRowsSatisfy(LESS_THAN, queryValue) is true, then no value in [min, max]
     * is less than queryValue. We verify with a witness value in [min, max].
     */
    @Property(tries = 10)
    void lessThanPruneSafety_noWitnessInRangeSatisfies(
            @ForAll("statsWithWitness") StatsWithWitness sw) {
        boolean pruned = sw.stats.noRowsSatisfy(ComparisonOp.LESS_THAN, sw.queryValue);
        if (pruned) {
            // Safety: witness (in [min, max]) must NOT be < queryValue
            assertThat(compareLongs(sw.witness, sw.queryValue) < 0)
                    .as("LT prune claimed safe but witness %s < query %s (range [%s, %s])",
                            sw.witness, sw.queryValue, sw.stats.minValue(), sw.stats.maxValue())
                    .isFalse();
        }
    }

    // ── LESS_THAN_OR_EQUAL safety ───────────────────────────────────────

    @Property(tries = 10)
    void lessThanOrEqualPruneSafety_noWitnessInRangeSatisfies(
            @ForAll("statsWithWitness") StatsWithWitness sw) {
        boolean pruned = sw.stats.noRowsSatisfy(ComparisonOp.LESS_THAN_OR_EQUAL, sw.queryValue);
        if (pruned) {
            // Safety: witness (in [min, max]) must NOT be <= queryValue
            assertThat(compareLongs(sw.witness, sw.queryValue) <= 0)
                    .as("LTE prune claimed safe but witness %s <= query %s (range [%s, %s])",
                            sw.witness, sw.queryValue, sw.stats.minValue(), sw.stats.maxValue())
                    .isFalse();
        }
    }

    // ── GREATER_THAN safety ─────────────────────────────────────────────

    @Property(tries = 10)
    void greaterThanPruneSafety_noWitnessInRangeSatisfies(
            @ForAll("statsWithWitness") StatsWithWitness sw) {
        boolean pruned = sw.stats.noRowsSatisfy(ComparisonOp.GREATER_THAN, sw.queryValue);
        if (pruned) {
            // Safety: witness (in [min, max]) must NOT be > queryValue
            assertThat(compareLongs(sw.witness, sw.queryValue) > 0)
                    .as("GT prune claimed safe but witness %s > query %s (range [%s, %s])",
                            sw.witness, sw.queryValue, sw.stats.minValue(), sw.stats.maxValue())
                    .isFalse();
        }
    }

    // ── GREATER_THAN_OR_EQUAL safety ────────────────────────────────────

    @Property(tries = 10)
    void greaterThanOrEqualPruneSafety_noWitnessInRangeSatisfies(
            @ForAll("statsWithWitness") StatsWithWitness sw) {
        boolean pruned = sw.stats.noRowsSatisfy(ComparisonOp.GREATER_THAN_OR_EQUAL, sw.queryValue);
        if (pruned) {
            // Safety: witness (in [min, max]) must NOT be >= queryValue
            assertThat(compareLongs(sw.witness, sw.queryValue) >= 0)
                    .as("GTE prune claimed safe but witness %s >= query %s (range [%s, %s])",
                            sw.witness, sw.queryValue, sw.stats.minValue(), sw.stats.maxValue())
                    .isFalse();
        }
    }

    // ── Null stats safety ───────────────────────────────────────────────

    /**
     * When min or max is null, noRowsSatisfy must always return false (conservative).
     */
    @Property(tries = 10)
    void nullStatsSafety_neverPrunes(
            @ForAll("comparisonOp") ComparisonOp op,
            @ForAll("queryLong") Long queryValue) {
        FileColumnStatistics nullMin = new FileColumnStatistics(
                "col", null, 100L, 0, 100, 10, 1024);
        FileColumnStatistics nullMax = new FileColumnStatistics(
                "col", 0L, null, 0, 100, 10, 1024);
        FileColumnStatistics nullBoth = new FileColumnStatistics(
                "col", null, null, 0, 100, 10, 1024);

        assertThat(nullMin.noRowsSatisfy(op, queryValue))
                .as("null minValue must never prune").isFalse();
        assertThat(nullMax.noRowsSatisfy(op, queryValue))
                .as("null maxValue must never prune").isFalse();
        assertThat(nullBoth.noRowsSatisfy(op, queryValue))
                .as("null min+max must never prune").isFalse();
    }

    // ── InList safety ───────────────────────────────────────────────────

    /**
     * InList prune safety: if all values in the list are pruned by EQUAL,
     * then no value in [min, max] equals any list value.
     */
    @Property(tries = 10)
    void inListPruneSafety_allValuesPrunedMeansNoneInRange(
            @ForAll("statsAndInList") StatsAndInList sil) {
        boolean allPruned = sil.listValues.stream()
                .allMatch(v -> sil.stats.noRowsSatisfy(ComparisonOp.EQUAL, v));
        if (allPruned) {
            // Safety: every list value must be outside [min, max]
            for (Comparable<?> v : sil.listValues) {
                @SuppressWarnings("unchecked")
                Comparable<Object> val = (Comparable<Object>) (Object) v;
                @SuppressWarnings("unchecked")
                Comparable<Object> min = (Comparable<Object>) sil.stats.minValue();
                @SuppressWarnings("unchecked")
                Comparable<Object> max = (Comparable<Object>) sil.stats.maxValue();
                assertThat(val.compareTo(min) < 0 || val.compareTo(max) > 0)
                        .as("InList prune claimed safe but value %s is within [%s, %s]",
                                v, sil.stats.minValue(), sil.stats.maxValue())
                        .isTrue();
            }
        }
    }

    // ── Arbitraries ─────────────────────────────────────────────────────

    @Provide
    Arbitrary<StatsAndQuery> statsAndQuery() {
        return Arbitraries.longs().between(-10_000, 10_000).flatMap(min ->
                Arbitraries.longs().between(min, min + 1000).flatMap(max ->
                        Arbitraries.longs().between(min - 500, max + 500)
                                .map(q -> new StatsAndQuery(
                                        new FileColumnStatistics("col", min, max, 0, 100, 10, 1024),
                                        q))));
    }

    @Provide
    Arbitrary<StatsWithWitness> statsWithWitness() {
        return Arbitraries.longs().between(-10_000, 10_000).flatMap(min ->
                Arbitraries.longs().between(min, min + 1000).flatMap(max ->
                        Arbitraries.longs().between(min, max).flatMap(witness ->
                                Arbitraries.longs().between(min - 500, max + 500)
                                        .map(q -> new StatsWithWitness(
                                                new FileColumnStatistics("col", min, max, 0, 100, 10, 1024),
                                                q, witness)))));
    }

    @Provide
    Arbitrary<StatsAndInList> statsAndInList() {
        return Arbitraries.longs().between(-10_000, 10_000).flatMap(min ->
                Arbitraries.longs().between(min, min + 1000).flatMap(max ->
                        Arbitraries.longs().between(min - 500, max + 500)
                                .list().ofMinSize(1).ofMaxSize(8)
                                .map(vals -> {
                                    List<Comparable<?>> comparables = new java.util.ArrayList<>(
                                            vals.stream()
                                                    .<Comparable<?>>map(v -> v)
                                                    .toList());
                                    return new StatsAndInList(
                                            new FileColumnStatistics("col", min, max, 0, 100, 10, 1024),
                                            comparables);
                                })));
    }

    @Provide
    Arbitrary<ComparisonOp> comparisonOp() {
        return Arbitraries.of(ComparisonOp.values());
    }

    @Provide
    Arbitrary<Long> queryLong() {
        return Arbitraries.longs().between(-10_000, 10_000);
    }

    @SuppressWarnings("unchecked")
    private static int compareLongs(Comparable<?> a, Comparable<?> b) {
        return ((Comparable<Object>) a).compareTo(b);
    }

    // ── Data carriers ───────────────────────────────────────────────────

    record StatsAndQuery(FileColumnStatistics stats, Long queryValue) {}

    record StatsWithWitness(FileColumnStatistics stats, Long queryValue, Long witness) {}

    record StatsAndInList(FileColumnStatistics stats, List<Comparable<?>> listValues) {}
}
