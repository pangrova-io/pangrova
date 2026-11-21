package org.pangrova.observability.sdk.metrics.runtime;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link NativeMetricsPuller} backoff and staleness semantics.
 */
class NativeMetricsPullerPropertyTest {

    @Property(tries = 10)
    void consecutiveFailures_increaseBackoff(
            @ForAll @IntRange(min = 1, max = 10) int failureCount) {

        NativeMetricsPuller puller = new NativeMetricsPuller(
                timeout -> { throw new RuntimeException("simulated failure"); },
                1000L, 500L);

        for (int i = 0; i < failureCount; i++) {
            puller.pull();
        }

        assertThat(puller.getConsecutiveFailures()).isEqualTo(failureCount);
        long delay = puller.getNextPullDelayMs();
        // Backoff should be >= base interval
        assertThat(delay).isGreaterThanOrEqualTo(1000L);
        // Backoff should be capped at 60x base
        assertThat(delay).isLessThanOrEqualTo(60_000L);
    }

    @Property(tries = 10)
    void successfulPull_resetsBackoff(
            @ForAll @IntRange(min = 1, max = 5) int failureCount) {

        byte[] fakeStats = new byte[]{1, 2, 3};
        NativeMetricsPuller puller = new NativeMetricsPuller(
                timeout -> { throw new RuntimeException("fail"); },
                1000L, 500L);

        // Accumulate failures
        for (int i = 0; i < failureCount; i++) {
            puller.pull();
        }
        assertThat(puller.getConsecutiveFailures()).isEqualTo(failureCount);

        // Replace with successful supplier and pull
        NativeMetricsPuller successPuller = new NativeMetricsPuller(
                timeout -> fakeStats, 1000L, 500L);
        byte[] result = successPuller.pull();

        assertThat(result).isEqualTo(fakeStats);
        assertThat(successPuller.getConsecutiveFailures()).isEqualTo(0);
        assertThat(successPuller.getNextPullDelayMs()).isEqualTo(1000L);
    }

    @Property(tries = 10)
    void backoffMultiplier_neverExceeds60x(
            @ForAll @IntRange(min = 1, max = 100) int failureCount) {

        NativeMetricsPuller puller = new NativeMetricsPuller(
                timeout -> { throw new RuntimeException("fail"); },
                100L, 50L);

        for (int i = 0; i < failureCount; i++) {
            puller.pull();
        }

        assertThat(puller.getNextPullDelayMs()).isLessThanOrEqualTo(6000L); // 100 * 60
    }
}
