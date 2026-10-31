package org.pangrova.streaming.property.cdc;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.streaming.core.cdc.CdcQuotaEnforcer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 18: CdcQuotaEnforcer counter balance invariant.
 *
 * <p>Validates: Requirements 17.3</p>
 *
 * <p>Verifies that onWriteStart/onWriteComplete keeps activeWrites and
 * pendingBytes non-negative after any sequence of balanced calls.</p>
 */
class QuotaCounterBalancePropertyTest {

    /**
     * Validates: Requirements 17.3
     *
     * After N balanced onWriteStart/onWriteComplete pairs,
     * activeWrites and pendingBytes return to zero.
     */
    @Property(tries = 10)
    void balancedStartCompleteReturnsToZero(
            @ForAll @IntRange(min = 1, max = 50) int writeCount,
            @ForAll @LongRange(min = 1, max = 100_000) long bytesPerWrite) {

        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(writeCount + 1, Long.MAX_VALUE);

        for (int i = 0; i < writeCount; i++) {
            quota.onWriteStart(bytesPerWrite);
        }

        assertThat(quota.activeWrites()).isEqualTo(writeCount);
        assertThat(quota.pendingBytes()).isEqualTo((long) writeCount * bytesPerWrite);

        for (int i = 0; i < writeCount; i++) {
            quota.onWriteComplete(bytesPerWrite);
        }

        assertThat(quota.activeWrites())
                .as("activeWrites must be zero after balanced start/complete")
                .isZero();
        assertThat(quota.pendingBytes())
                .as("pendingBytes must be zero after balanced start/complete")
                .isZero();
    }

    /**
     * Validates: Requirements 17.3
     *
     * After each onWriteStart, activeWrites and pendingBytes are non-negative.
     * After each onWriteComplete, they remain non-negative.
     */
    @Property(tries = 10)
    void countersNeverNegativeDuringBalancedSequence(
            @ForAll("byteSizes") List<Long> byteSizes) {
        Assume.that(!byteSizes.isEmpty());

        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(byteSizes.size() + 1, Long.MAX_VALUE);

        // Start all writes
        for (long bytes : byteSizes) {
            quota.onWriteStart(bytes);
            assertThat(quota.activeWrites())
                    .as("activeWrites must be non-negative after onWriteStart")
                    .isGreaterThanOrEqualTo(0);
            assertThat(quota.pendingBytes())
                    .as("pendingBytes must be non-negative after onWriteStart")
                    .isGreaterThanOrEqualTo(0);
        }

        // Complete all writes
        for (long bytes : byteSizes) {
            quota.onWriteComplete(bytes);
            assertThat(quota.activeWrites())
                    .as("activeWrites must be non-negative after onWriteComplete")
                    .isGreaterThanOrEqualTo(0);
            assertThat(quota.pendingBytes())
                    .as("pendingBytes must be non-negative after onWriteComplete")
                    .isGreaterThanOrEqualTo(0);
        }
    }

    /**
     * Validates: Requirements 17.3
     *
     * Interleaved start/complete calls maintain non-negative counters.
     * Pattern: start(a), start(b), complete(a), start(c), complete(b), complete(c)
     */
    @Property(tries = 10)
    void interleavedStartCompleteKeepsCountersNonNegative(
            @ForAll("byteSizes") List<Long> byteSizes) {
        Assume.that(byteSizes.size() >= 2);

        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(byteSizes.size() + 1, Long.MAX_VALUE);

        // Interleave: start first half, then alternate complete/start for second half
        int half = byteSizes.size() / 2;
        List<Long> firstHalf = byteSizes.subList(0, half);
        List<Long> secondHalf = byteSizes.subList(half, byteSizes.size());

        for (long bytes : firstHalf) {
            quota.onWriteStart(bytes);
        }

        // Complete first half while starting second half
        int completeIdx = 0;
        for (long bytes : secondHalf) {
            if (completeIdx < firstHalf.size()) {
                quota.onWriteComplete(firstHalf.get(completeIdx++));
            }
            quota.onWriteStart(bytes);
            assertThat(quota.activeWrites()).isGreaterThanOrEqualTo(0);
            assertThat(quota.pendingBytes()).isGreaterThanOrEqualTo(0);
        }

        // Complete remaining from first half
        while (completeIdx < firstHalf.size()) {
            quota.onWriteComplete(firstHalf.get(completeIdx++));
        }

        // Complete second half
        for (long bytes : secondHalf) {
            quota.onWriteComplete(bytes);
            assertThat(quota.activeWrites()).isGreaterThanOrEqualTo(0);
            assertThat(quota.pendingBytes()).isGreaterThanOrEqualTo(0);
        }

        assertThat(quota.activeWrites()).isZero();
        assertThat(quota.pendingBytes()).isZero();
    }

    /**
     * Validates: Requirements 17.3
     *
     * shouldPause() correctly reflects when limits are exceeded.
     */
    @Property(tries = 10)
    void shouldPauseReflectsLimits(
            @ForAll @IntRange(min = 1, max = 10) int maxConcurrent,
            @ForAll @LongRange(min = 100, max = 10_000) long maxBytes,
            @ForAll @LongRange(min = 1, max = 500) long writeBytes) {

        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(maxConcurrent, maxBytes);
        assertThat(quota.shouldPause()).isFalse();

        // Fill up to the limit
        long totalBytes = 0;
        for (int i = 0; i < maxConcurrent; i++) {
            quota.onWriteStart(writeBytes);
            totalBytes += writeBytes;
        }

        // Should pause because activeWrites >= maxConcurrent
        assertThat(quota.shouldPause()).isTrue();

        // Complete all
        for (int i = 0; i < maxConcurrent; i++) {
            quota.onWriteComplete(writeBytes);
        }

        assertThat(quota.shouldPause()).isFalse();
        assertThat(quota.activeWrites()).isZero();
        assertThat(quota.pendingBytes()).isZero();
    }

    @Provide
    Arbitrary<List<Long>> byteSizes() {
        return Arbitraries.longs().between(1, 50_000)
                .list().ofMinSize(2).ofMaxSize(20);
    }
}
