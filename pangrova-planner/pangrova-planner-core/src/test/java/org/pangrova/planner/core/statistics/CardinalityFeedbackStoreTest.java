package org.pangrova.planner.core.statistics;

import org.pangrova.planner.api.statistics.CardinalityFeedback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

class CardinalityFeedbackStoreTest {

    private static final Offset<Double> EPS = Offset.offset(1e-9);

    @Test
    void noHistoryLeavesEstimateUnchanged() {
        CardinalityFeedbackStore store = new CardinalityFeedbackStore();
        assertThat(store.correctedEstimate("shapeA", 1000)).isEqualTo(1000, EPS);
        assertThat(store.correctionFactor("shapeA")).isEmpty();
        assertThat(store.get("shapeA")).isEmpty();
    }

    @Test
    void firstObservationSeedsRawRatio() {
        CardinalityFeedbackStore store = new CardinalityFeedbackStore();
        store.record("shapeA", 100, 400); // ratio 4.0
        assertThat(store.correctionFactor("shapeA")).hasValue(4.0);
        assertThat(store.get("shapeA")).get()
                .extracting(CardinalityFeedback::observationCount).isEqualTo(1L);
    }

    @Test
    void correctedEstimateAppliesFactor() {
        CardinalityFeedbackStore store = new CardinalityFeedbackStore();
        store.record("shapeA", 100, 400); // factor 4.0
        assertThat(store.correctedEstimate("shapeA", 250)).isEqualTo(1000, EPS);
    }

    @Test
    void repeatedConsistentFeedbackConvergesToActual() {
        // Estimate persistently under-counts 10x. Over repeats the corrected estimate should
        // converge to the actual (1000), demonstrating history-based convergence.
        CardinalityFeedbackStore store = new CardinalityFeedbackStore(0.5);
        long estimate = 100;
        long actual = 1000;
        double corrected = 0;
        for (int i = 0; i < 40; i++) {
            store.record("shapeA", estimate, actual);
            corrected = store.correctedEstimate("shapeA", estimate);
        }
        assertThat(corrected).isCloseTo(1000.0, Offset.offset(0.01));
        assertThat(store.get("shapeA")).get()
                .extracting(CardinalityFeedback::observationCount).isEqualTo(40L);
    }

    @Test
    void smoothingDampensSingleOutlier() {
        // Steady ratio 1.0, then one outlier of 11.0 with alpha 0.3 → 0.3*11 + 0.7*1 = 4.0,
        // not the raw 11.0 — the outlier is dampened.
        CardinalityFeedbackStore store = new CardinalityFeedbackStore(0.3);
        store.record("shapeA", 100, 100); // ratio 1.0 seed
        store.record("shapeA", 100, 1100); // ratio 11.0
        assertThat(store.correctionFactor("shapeA")).hasValue(4.0);
    }

    @Test
    void nonPositiveCountsAreIgnored() {
        CardinalityFeedbackStore store = new CardinalityFeedbackStore();
        store.record("shapeA", 0, 100);
        store.record("shapeA", 100, 0);
        store.record("shapeA", -5, -5);
        assertThat(store.get("shapeA")).isEmpty();
    }

    @Test
    void distinctShapesTrackedIndependently() {
        CardinalityFeedbackStore store = new CardinalityFeedbackStore();
        store.record("shapeA", 100, 200); // 2.0
        store.record("shapeB", 100, 50);  // 0.5
        assertThat(store.correctionFactor("shapeA")).hasValue(2.0);
        assertThat(store.correctionFactor("shapeB")).hasValue(0.5);
        assertThat(store.size()).isEqualTo(2);
    }

    @Test
    void clearRemovesAllHistory() {
        CardinalityFeedbackStore store = new CardinalityFeedbackStore();
        store.record("shapeA", 100, 200);
        store.clear();
        assertThat(store.size()).isZero();
        assertThat(store.get("shapeA")).isEmpty();
    }

    @Test
    void constructorRejectsAlphaOutOfRange() {
        assertThatThrownBy(() -> new CardinalityFeedbackStore(0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CardinalityFeedbackStore(1.5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void blankKeyRejected() {
        CardinalityFeedbackStore store = new CardinalityFeedbackStore();
        assertThatThrownBy(() -> store.record(" ", 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.correctionFactor(""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
