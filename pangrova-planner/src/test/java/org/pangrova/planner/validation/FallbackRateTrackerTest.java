package org.pangrova.planner.validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FallbackRateTracker}.
 * Task 198 + 201: validates fallback-rate metric tracking.
 */
class FallbackRateTrackerTest {

    private FallbackRateTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new FallbackRateTracker();
    }

    @Test
    void initialStateIsZero() {
        assertThat(tracker.totalPlans()).isZero();
        assertThat(tracker.fallbackPlans()).isZero();
        assertThat(tracker.fallbackRate()).isEqualTo(0.0);
    }

    @Test
    void fullyNativeDoesNotIncrementFallback() {
        tracker.recordFullyNative();
        tracker.recordFullyNative();

        assertThat(tracker.totalPlans()).isEqualTo(2);
        assertThat(tracker.fallbackPlans()).isZero();
        assertThat(tracker.fallbackRate()).isEqualTo(0.0);
    }

    @Test
    void partialFallbackIncrementsCorrectly() {
        tracker.recordFullyNative();
        tracker.recordPartialFallback();

        assertThat(tracker.totalPlans()).isEqualTo(2);
        assertThat(tracker.fallbackPlans()).isEqualTo(1);
        assertThat(tracker.fallbackRate()).isEqualTo(0.5);
    }

    @Test
    void nativeUnsupportedTrackedSeparately() {
        tracker.recordNativeUnsupported();

        assertThat(tracker.totalPlans()).isEqualTo(1);
        assertThat(tracker.nativeUnsupportedPlans()).isEqualTo(1);
        assertThat(tracker.fallbackPlans()).isZero();
    }

    @Test
    void resetClearsAllCounters() {
        tracker.recordFullyNative();
        tracker.recordPartialFallback();
        tracker.recordNativeUnsupported();
        tracker.reset();

        assertThat(tracker.totalPlans()).isZero();
        assertThat(tracker.fallbackPlans()).isZero();
        assertThat(tracker.nativeUnsupportedPlans()).isZero();
    }

    @Test
    void mixedTrafficComputesCorrectRate() {
        // 7 fully native, 3 partial fallback = 30% rate
        for (int i = 0; i < 7; i++) {
            tracker.recordFullyNative();
        }
        for (int i = 0; i < 3; i++) {
            tracker.recordPartialFallback();
        }

        assertThat(tracker.totalPlans()).isEqualTo(10);
        assertThat(tracker.fallbackRate()).isCloseTo(0.3, org.assertj.core.data.Offset.offset(0.001));
    }
}
