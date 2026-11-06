package org.pangrova.coordinator.property.index;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.indexing.filter.AdaptivePruningCostModel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property P8: Cost-Based Selection Conservatism.
 *
 * <p>∀ index i, ∀ predicate p: if {@code shouldSkipL1()} returns true,
 * then skipping L1 never changes query correctness — it only avoids
 * unnecessary evaluation overhead.</p>
 *
 * <p>The key invariant: skipping is conservative. When L0 already pruned >95%,
 * coverage is <50%, or selectivity is >50%, the remaining L1 evaluation
 * would provide diminishing returns. Skipping always defaults to KEEP
 * (never SKIP), so no data is lost.</p>
 *
 * <p>Validates: Requirements 5.AC3, 5.AC4</p>
 */
class CostBasedSelectionPropertyTest {

    private final AdaptivePruningCostModel costModel = new AdaptivePruningCostModel();

    /**
     * When shouldSkipL1 returns true, at least one skip condition is met.
     */
    @Property(tries = 10)
    void skipDecisionImpliesAtLeastOneThresholdExceeded(
            @ForAll("costParams") CostParams params) {
        boolean skip = costModel.shouldSkipL1(
                params.l0Ratio, params.coverage, params.selectivity);
        if (skip) {
            boolean l0High = params.l0Ratio > 0.95;
            boolean coverageLow = params.coverage < 0.50;
            boolean selectivityHigh = params.selectivity > 0.50;
            assertThat(l0High || coverageLow || selectivityHigh)
                    .as("Skip must be justified: l0=%.2f cov=%.2f sel=%.2f",
                            params.l0Ratio, params.coverage, params.selectivity)
                    .isTrue();
        }
    }

    /**
     * When all thresholds are within safe bounds, L1 is never skipped.
     */
    @Property(tries = 10)
    void safeParamsNeverSkip(
            @ForAll("safeParams") CostParams params) {
        boolean skip = costModel.shouldSkipL1(
                params.l0Ratio, params.coverage, params.selectivity);
        assertThat(skip)
                .as("Safe params must not skip: l0=%.2f cov=%.2f sel=%.2f",
                        params.l0Ratio, params.coverage, params.selectivity)
                .isFalse();
    }

    /**
     * Selectivity estimation returns value in [0, 1] range.
     */
    @Property(tries = 10)
    void selectivityEstimateInValidRange(
            @ForAll("ndvValues") long ndv) {
        var stats = new org.pangrova.connector.api.index.statistics.FileColumnStatistics(
                "col", 0L, 100L, 0, 100, ndv, 1024);
        double sel = costModel.estimateSelectivity(
                java.util.List.of(stats), null);
        assertThat(sel).isBetween(0.0, 1.0);
    }

    @Provide
    Arbitrary<CostParams> costParams() {
        return Combinators.combine(
                Arbitraries.doubles().between(0.0, 1.0),
                Arbitraries.doubles().between(0.0, 1.0),
                Arbitraries.doubles().between(0.0, 1.0)
        ).as(CostParams::new);
    }

    @Provide
    Arbitrary<CostParams> safeParams() {
        return Combinators.combine(
                Arbitraries.doubles().between(0.0, 0.94),
                Arbitraries.doubles().between(0.51, 1.0),
                Arbitraries.doubles().between(0.0, 0.49)
        ).as(CostParams::new);
    }

    @Provide
    Arbitrary<Long> ndvValues() {
        return Arbitraries.longs().between(1, 1_000_000);
    }

    record CostParams(double l0Ratio, double coverage, double selectivity) {}
}
