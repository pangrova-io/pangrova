package org.pangrova.planner.rule;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class CTEMaterializationRuleTest {

    // --- shouldMaterialize (fallback, no row estimate) ---

    @Test
    void referenceCountOne_alwaysInline() {
        assertThat(CTEMaterializationRule.shouldMaterialize(50_000, 1)).isFalse();
    }

    @Test
    void referenceCountZero_inline() {
        assertThat(CTEMaterializationRule.shouldMaterialize(50_000, 0)).isFalse();
    }

    @Test
    void referenceTwice_lowCost_inline() {
        // inline = 500 * 2 = 1000, materialize = 500 + 1000(fallback) = 1500 → inline
        assertThat(CTEMaterializationRule.shouldMaterialize(500, 2)).isFalse();
    }

    @Test
    void referenceTwice_highCost_materialize() {
        // inline = 5000 * 2 = 10000, materialize = 5000 + 1000(fallback) = 6000 → materialize
        assertThat(CTEMaterializationRule.shouldMaterialize(5000, 2)).isTrue();
    }

    @Test
    void referenceFiveTimes_materialize() {
        // inline = 500 * 5 = 2500, materialize = 500 + 1000(fallback) = 1500 → materialize
        assertThat(CTEMaterializationRule.shouldMaterialize(500, 5)).isTrue();
    }

    @Test
    void exactBreakEvenPoint_materialize() {
        // cteCost = 1000, refCount = 2 → materialize = 2000, inline = 2000 → equal cost → materialize
        // (materializing is preferred at break-even: compute once + read is better than compute twice)
        assertThat(CTEMaterializationRule.shouldMaterialize(1000, 2)).isTrue();
    }

    @Test
    void justAboveBreakEven_materialize() {
        // cteCost = 1001, refCount = 2 → materialize = 2001, inline = 2002 → materialize
        assertThat(CTEMaterializationRule.shouldMaterialize(1001, 2)).isTrue();
    }

    // --- shouldMaterialize (dynamic, with row estimate) ---

    @Test
    void withRowEstimate_smallCte_inline() {
        // 100 rows → overhead = 100 + 100*0.05 = 105
        // inline = 200 * 2 = 400, materialize = 200 + 105 = 305 → materialize
        assertThat(CTEMaterializationRule.shouldMaterialize(200, 2, 100)).isTrue();
    }

    @Test
    void withRowEstimate_largeCte_higherOverhead() {
        // 1M rows → overhead = 100 + 1_000_000*0.05 = 50_100
        // inline = 40_000 * 2 = 80_000, materialize = 40_000 + 50_100 = 90_100 → inline
        assertThat(CTEMaterializationRule.shouldMaterialize(40_000, 2, 1_000_000)).isFalse();
    }

    @Test
    void withRowEstimate_largeCte_manyRefs_materialize() {
        // 1M rows → overhead = 50_100
        // inline = 40_000 * 5 = 200_000, materialize = 40_000 + 50_100 = 90_100 → materialize
        assertThat(CTEMaterializationRule.shouldMaterialize(40_000, 5, 1_000_000)).isTrue();
    }

    @Test
    void negativeRowEstimate_usesFallback() {
        assertThat(CTEMaterializationRule.shouldMaterialize(5000, 2, -1)).isTrue();
    }

    // --- estimateOverhead ---

    @Test
    void overheadScalesWithRowCount() {
        // 10_000 rows → 100 + 10_000 * 0.05 = 600
        assertThat(CTEMaterializationRule.estimateOverhead(10_000))
                .isCloseTo(600.0, within(0.001));
    }

    @Test
    void overheadFallbackWhenNoEstimate() {
        assertThat(CTEMaterializationRule.estimateOverhead(-1))
                .isEqualTo(CTEMaterializationRule.FALLBACK_OVERHEAD);
    }

    // --- estimateMaterializationCost ---

    @Test
    void materializationCost_addsDynamicOverhead() {
        // 5000 rows → overhead = 100 + 250 = 350
        assertThat(CTEMaterializationRule.estimateMaterializationCost(3000, 5000))
                .isCloseTo(3350.0, within(0.001));
    }

    // --- estimateInlineCost ---

    @Test
    void inlineCost_multipliedByRefCount() {
        assertThat(CTEMaterializationRule.estimateInlineCost(3000, 4)).isEqualTo(12000);
    }
}
