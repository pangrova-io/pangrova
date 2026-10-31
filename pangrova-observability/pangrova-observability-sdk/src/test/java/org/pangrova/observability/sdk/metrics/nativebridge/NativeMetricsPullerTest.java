package org.pangrova.observability.sdk.metrics.nativebridge;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests NativeMetricsPuller backoff logic on consecutive failures.
 * Uses real test implementations of NativeStatsSupplier (no mocks).
 *
 * Validates: Requirements 17.1
 */
class NativeMetricsPullerTest {

    @Test
    void supplierAndReturnedArraysCannotMutateLastSuccessfulSnapshot() {
        byte[] supplied = {1, 2, 3};
        var puller = new NativeMetricsPuller(timeoutMs -> supplied);
        byte[] pulled = puller.pull();
        supplied[0] = 9;
        pulled[1] = 8;
        byte[] inspected = puller.getLastStats();
        inspected[2] = 7;

        assertThat(puller.getLastStats()).containsExactly(1, 2, 3);
        assertThat(pulled).isNotSameAs(supplied);
        assertThat(puller.getLastStats()).isNotSameAs(inspected);
    }

    @Test
    void nullSupplierPayloadRemainsNull() {
        var puller = new NativeMetricsPuller(timeoutMs -> null);
        assertThat(puller.getLastStats()).isNull();
        assertThat(puller.pull()).isNull();
        assertThat(puller.getLastStats()).isNull();
        assertThat(puller.getConsecutiveFailures()).isZero();
    }

    @Test
    void successfulPullReturnsStatsAndResetsFailures() {
        byte[] expected = {1, 2, 3};
        var puller = new NativeMetricsPuller(timeoutMs -> expected);

        byte[] result = puller.pull();

        assertThat(result).isEqualTo(expected);
        assertThat(puller.getConsecutiveFailures()).isZero();
        assertThat(puller.getLastStats()).isEqualTo(expected);
    }

    @Test
    void failedPullReturnsNullAndIncrementsFailures() {
        var puller = new NativeMetricsPuller(timeoutMs -> {
            throw new RuntimeException("native crash");
        });

        byte[] result = puller.pull();

        assertThat(result).isNull();
        assertThat(puller.getConsecutiveFailures()).isEqualTo(1);
    }

    @Test
    void consecutiveFailuresAccumulate() {
        var puller = new NativeMetricsPuller(timeoutMs -> {
            throw new RuntimeException("fail");
        });

        puller.pull();
        puller.pull();
        puller.pull();

        assertThat(puller.getConsecutiveFailures()).isEqualTo(3);
    }

    @Test
    void successAfterFailuresResetsCounter() {
        var callCount = new int[]{0};
        var puller = new NativeMetricsPuller(timeoutMs -> {
            callCount[0]++;
            if (callCount[0] <= 2) {
                throw new RuntimeException("fail");
            }
            return new byte[]{42};
        });

        puller.pull();
        puller.pull();
        assertThat(puller.getConsecutiveFailures()).isEqualTo(2);

        byte[] result = puller.pull();
        assertThat(result).containsExactly(42);
        assertThat(puller.getConsecutiveFailures()).isZero();
    }

    @Test
    void nextPullDelayIsBaseIntervalOnZeroFailures() {
        var puller = new NativeMetricsPuller(timeoutMs -> new byte[0], 1000L, 500L);

        puller.pull();

        assertThat(puller.getNextPullDelayMs()).isEqualTo(1000L);
    }

    @Test
    void nextPullDelayDoublesPerFailure() {
        var puller = new NativeMetricsPuller(
                timeoutMs -> { throw new RuntimeException("fail"); },
                1000L, 500L);

        puller.pull(); // 1 failure -> 2^1 = 2x
        assertThat(puller.getNextPullDelayMs()).isEqualTo(2000L);

        puller.pull(); // 2 failures -> 2^2 = 4x
        assertThat(puller.getNextPullDelayMs()).isEqualTo(4000L);

        puller.pull(); // 3 failures -> 2^3 = 8x
        assertThat(puller.getNextPullDelayMs()).isEqualTo(8000L);
    }

    @Test
    void nextPullDelayCapsAtSixtyTimesBaseInterval() {
        var puller = new NativeMetricsPuller(
                timeoutMs -> { throw new RuntimeException("fail"); },
                1000L, 500L);

        // 2^7 = 128 > 60, so should cap at 60x
        for (int i = 0; i < 7; i++) {
            puller.pull();
        }

        assertThat(puller.getNextPullDelayMs()).isEqualTo(60_000L);
    }

    @Test
    void isStaleReturnsFalseImmediatelyAfterSuccess() {
        var puller = new NativeMetricsPuller(timeoutMs -> new byte[0], 1000L, 500L);

        puller.pull();

        assertThat(puller.isStale()).isFalse();
    }

    @Test
    void lastStatsPreservedAcrossFailedPulls() {
        byte[] good = {10, 20};
        var callCount = new int[]{0};
        var puller = new NativeMetricsPuller(timeoutMs -> {
            callCount[0]++;
            if (callCount[0] == 1) {
                return good;
            }
            throw new RuntimeException("fail");
        });

        puller.pull(); // success
        puller.pull(); // failure

        assertThat(puller.getLastStats()).isEqualTo(good);
        assertThat(puller.getConsecutiveFailures()).isEqualTo(1);
    }
}
