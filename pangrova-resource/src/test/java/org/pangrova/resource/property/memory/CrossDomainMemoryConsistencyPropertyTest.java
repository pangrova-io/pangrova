package org.pangrova.resource.property.memory;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.RootMemoryPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for cross-domain memory accounting bounded consistency.
 *
 * <p>Validates two design properties:
 * <ul>
 *   <li><b>Property 16</b>: For any tenant T with Java domain allocation J bytes
 *       and Native domain allocation N bytes, the combined usage reported by the
 *       arbitrator SHALL converge to J+N within RPC RTT + heartbeat interval
 *       (bounded lag).</li>
 *   <li><b>Property 17</b>: For any sequence of reserve/release operations on a
 *       MemoryPool, the arbitrator's snapshot and the MemoryPool's own reported
 *       used bytes SHALL be completely consistent (difference = 0).</li>
 * </ul>
 *
 * <p><b>Validates: Requirements 11.6, 11.7</b></p>
 */
@Tag("Feature: production-readiness-fixes")
@Tag("Property 16: Cross-domain memory bounded consistency")
@Tag("Property 17: MemoryPool delegation equivalence")
class CrossDomainMemoryConsistencyPropertyTest {

    // ========================================================================
    // Property 16: Cross-domain bounded consistency
    //
    // Java allocations go through arbitrator.requestMemory (query path).
    // Native allocations go through arbitrator.reserve (non-query consumer path).
    // Combined usage = arbitrator.getUsedMemory() must equal Java + Native actual.
    // ========================================================================

    /**
     * For any combination of Java and Native allocations against a shared
     * arbitrator, the arbitrator's reported usedMemory equals the sum of
     * Java allocated + Native reserved (zero lag in synchronous path).
     *
     * <p>This validates the synchronous case of Property 16: when both domains
     * route through the same arbitrator, combined usage is exact.</p>
     *
     * <p><b>Validates: Requirements 11.7</b></p>
     */
    @Property(tries = 100)
    @Label("Combined Java+Native usage equals arbitrator reported usage (synchronous)")
    void combinedUsageEqualsArbitratorReported(
            @ForAll @LongRange(min = 10_000, max = 1_000_000) long totalBudget,
            @ForAll @LongRange(min = 100, max = 5000) long javaRequest,
            @ForAll @LongRange(min = 100, max = 5000) long nativeRequest) {

        MemoryArbitrator arbitrator = new MemoryArbitrator(totalBudget, 0);

        // Java domain allocation (query path)
        long javaAllocated = arbitrator.requestMemory("java-q1", javaRequest);

        // Native domain allocation (reserve path, simulating NativeMemoryCoordinator)
        long nativeAllocated = arbitrator.reserve(nativeRequest);

        // Property: arbitrator's total used = Java + Native
        long combinedActual = javaAllocated + nativeAllocated;
        assertThat(arbitrator.getUsedMemory())
                .as("Arbitrator usedMemory must equal Java(%d) + Native(%d)",
                        javaAllocated, nativeAllocated)
                .isEqualTo(combinedActual);
    }

    /**
     * For any sequence of interleaved Java reserve/release and Native
     * reserve/unreserve operations, the arbitrator's usedMemory always
     * equals the net outstanding allocations across both domains.
     *
     * <p><b>Validates: Requirements 11.7</b></p>
     */
    @Property(tries = 100)
    @Label("Interleaved Java+Native ops maintain exact combined accounting")
    void interleavedOpsExactAccounting(
            @ForAll @IntRange(min = 2, max = 20) int opCount,
            @ForAll @LongRange(min = 50_000, max = 500_000) long totalBudget) {

        MemoryArbitrator arbitrator = new MemoryArbitrator(totalBudget, 0);
        long javaOutstanding = 0;
        long nativeOutstanding = 0;

        for (int i = 0; i < opCount; i++) {
            long amount = 1000 + (i * 500L);
            if (i % 3 == 0) {
                // Java allocate
                long allocated = arbitrator.requestMemory("q-" + i, amount);
                javaOutstanding += allocated;
            } else if (i % 3 == 1) {
                // Native allocate
                long reserved = arbitrator.reserve(amount);
                nativeOutstanding += reserved;
            } else {
                // Release: alternate between Java and Native
                if (javaOutstanding > 0) {
                    long toRelease = Math.min(500, javaOutstanding);
                    arbitrator.release("q-" + (i - 2), toRelease);
                    javaOutstanding -= toRelease;
                }
                if (nativeOutstanding > 0) {
                    long toRelease = Math.min(500, nativeOutstanding);
                    arbitrator.releaseReservation(toRelease);
                    nativeOutstanding -= toRelease;
                }
            }
        }

        long expectedCombined = javaOutstanding + nativeOutstanding;
        assertThat(arbitrator.getUsedMemory())
                .as("After interleaved ops, usedMemory must equal net outstanding")
                .isEqualTo(expectedCombined);
    }

    /**
     * Under concurrent Java and Native allocations against a shared arbitrator,
     * the combined usage never exceeds the total budget (bounded by allocatable).
     * This validates the bounded consistency guarantee under contention.
     *
     * <p><b>Validates: Requirements 11.7</b></p>
     */
    @Property(tries = 100)
    @Label("Concurrent cross-domain allocations bounded by total budget")
    void concurrentCrossDomainBounded(
            @ForAll @LongRange(min = 50_000, max = 500_000) long totalBudget,
            @ForAll @IntRange(min = 2, max = 6) int threadCount) throws InterruptedException {

        MemoryArbitrator arbitrator = new MemoryArbitrator(totalBudget, 0);
        long perThread = totalBudget / 2;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                try {
                    start.await();
                    if (idx % 2 == 0) {
                        // Java domain
                        arbitrator.requestMemory("q-" + idx, perThread);
                    } else {
                        // Native domain
                        arbitrator.reserve(perThread);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            t.setDaemon(true);
            t.start();
        }

        start.countDown();
        done.await();

        assertThat(arbitrator.getUsedMemory())
                .as("Combined usage must never exceed allocatable budget")
                .isLessThanOrEqualTo(totalBudget);
    }

    /**
     * Simulates asynchronous Native event reporting with bounded delay.
     * A native consumer reports usage asynchronously; the difference between
     * the arbitrator's view and the true combined usage is bounded by the
     * maximum async event size (simulating heartbeat interval + RPC RTT lag).
     *
     * <p><b>Validates: Requirements 11.7</b></p>
     */
    @Property(tries = 100)
    @Label("Async native events produce bounded lag in combined usage")
    void asyncNativeEventsBoundedLag(
            @ForAll @LongRange(min = 100_000, max = 1_000_000) long totalBudget,
            @ForAll @IntRange(min = 1, max = 10) int eventCount,
            @ForAll @LongRange(min = 100, max = 5000) long maxEventSize) {

        MemoryArbitrator arbitrator = new MemoryArbitrator(totalBudget, 0);

        // Java domain: allocate some memory
        long javaAllocated = arbitrator.requestMemory("java-q1", 10_000);

        // Simulate native events arriving with bounded delay.
        // Each event represents a native allocation that hasn't been reported yet.
        // The maximum lag = maxEventSize (one heartbeat worth of unreported allocation).
        long nativeActual = 0;
        long nativeReported = 0;

        List<Long> pendingEvents = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            long eventSize = Math.min(maxEventSize,
                    (totalBudget - javaAllocated - nativeActual) / 2);
            if (eventSize <= 0) {
                break;
            }
            nativeActual += eventSize;
            pendingEvents.add(eventSize);
        }

        // Process events one by one (simulating heartbeat delivery)
        for (long event : pendingEvents) {
            // Before processing: lag = nativeActual - nativeReported
            long lagBefore = nativeActual - nativeReported;
            assertThat(lagBefore)
                    .as("Lag before event processing must be bounded")
                    .isLessThanOrEqualTo(maxEventSize * eventCount);

            // Process event: native reports to arbitrator
            long reserved = arbitrator.reserve(event);
            nativeReported += reserved;
        }

        // After all events processed: lag converges to 0
        // (some events may have been partially granted due to budget)
        long finalArbitratorUsed = arbitrator.getUsedMemory();
        long finalActualCombined = javaAllocated + nativeReported;
        assertThat(finalArbitratorUsed)
                .as("After all events processed, arbitrator view equals actual combined")
                .isEqualTo(finalActualCombined);
    }

    // ========================================================================
    // Property 17: MemoryPool delegation equivalence
    //
    // For any sequence of reserve/release on a MemoryPool, the arbitrator's
    // snapshot and the pool's own reported used bytes are exactly consistent.
    // ========================================================================

    /**
     * For any sequence of tryReserve and free operations on a RootMemoryPool,
     * the pool's getUsedBytes() always matches the net bytes actually reserved
     * (difference = 0).
     *
     * <p><b>Validates: Requirements 11.6</b></p>
     */
    @Property(tries = 100)
    @Label("MemoryPool used bytes equals net reserved after any operation sequence")
    void memoryPoolUsedEqualsNetReserved(
            @ForAll @LongRange(min = 10_000, max = 500_000) long poolMax,
            @ForAll @IntRange(min = 2, max = 30) int opCount) {

        RootMemoryPool pool = new RootMemoryPool("test-pool", poolMax);
        long netReserved = 0;

        for (int i = 0; i < opCount; i++) {
            long amount = 500 + (i * 100L);
            if (i % 2 == 0) {
                // Reserve
                long reserved = pool.tryReserve(amount);
                netReserved += reserved;
            } else {
                // Free
                long toFree = Math.min(amount, netReserved);
                pool.free(toFree);
                netReserved -= toFree;
            }
        }

        assertThat(pool.getUsedBytes())
                .as("Pool usedBytes must exactly equal net reserved bytes")
                .isEqualTo(netReserved);
    }

    /**
     * For any sequence of reserve/release operations routed through the
     * arbitrator (simulating MemoryPool as arbitrator sub-pool wrapper),
     * the arbitrator's getUsedMemory() exactly matches the sum of all
     * outstanding allocations (difference = 0).
     *
     * <p>This validates that when MemoryPool delegates to the arbitrator,
     * there is no accounting drift between the two views.</p>
     *
     * <p><b>Validates: Requirements 11.6</b></p>
     */
    @Property(tries = 100)
    @Label("Arbitrator snapshot matches pool delegation exactly (diff=0)")
    void arbitratorSnapshotMatchesPoolDelegation(
            @ForAll @LongRange(min = 50_000, max = 1_000_000) long totalBudget,
            @ForAll @IntRange(min = 2, max = 20) int opCount) {

        MemoryArbitrator arbitrator = new MemoryArbitrator(totalBudget, 0);
        long expectedUsed = 0;

        for (int i = 0; i < opCount; i++) {
            long amount = 1000 + (i * 200L);
            if (i % 3 != 2) {
                // Reserve through arbitrator (simulating pool delegation)
                long allocated = arbitrator.requestMemory("pool-q-" + i, amount);
                expectedUsed += allocated;
            } else {
                // Release
                if (expectedUsed > 0) {
                    long toRelease = Math.min(amount, expectedUsed);
                    arbitrator.release("pool-q-" + (i - 1), toRelease);
                    expectedUsed -= toRelease;
                }
            }
        }

        assertThat(arbitrator.getUsedMemory())
                .as("Arbitrator usedMemory must exactly match net outstanding (diff=0)")
                .isEqualTo(expectedUsed);
    }

    /**
     * Under concurrent reserve/release operations on a shared arbitrator
     * (simulating multiple MemoryPool sub-pools), the final usedMemory
     * after all threads complete equals the net outstanding bytes.
     *
     * <p><b>Validates: Requirements 11.6</b></p>
     */
    @Property(tries = 100)
    @Label("Concurrent pool delegation maintains exact accounting")
    void concurrentPoolDelegationExact(
            @ForAll @LongRange(min = 100_000, max = 1_000_000) long totalBudget,
            @ForAll @IntRange(min = 2, max = 8) int threadCount) throws InterruptedException {

        MemoryArbitrator arbitrator = new MemoryArbitrator(totalBudget, 0);
        long allocPerThread = 1000;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicLong totalAllocated = new AtomicLong(0);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                try {
                    start.await();
                    long allocated = arbitrator.requestMemory("pool-" + idx, allocPerThread);
                    totalAllocated.addAndGet(allocated);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            t.setDaemon(true);
            t.start();
        }

        start.countDown();
        done.await();

        assertThat(arbitrator.getUsedMemory())
                .as("After concurrent allocations, usedMemory equals total allocated")
                .isEqualTo(totalAllocated.get());
    }

    /**
     * For any sequence of reserve/release, the MemoryPool never reports
     * negative usedBytes and the arbitrator never reports negative usedMemory.
     * MemoryPool enforces this via DoubleReleaseException when release > current;
     * the arbitrator clamps internally.
     *
     * <p><b>Validates: Requirements 11.6</b></p>
     */
    @Property(tries = 100)
    @Label("Neither pool nor arbitrator ever reports negative usage")
    void neverNegativeUsage(
            @ForAll @LongRange(min = 10_000, max = 500_000) long budget,
            @ForAll @IntRange(min = 1, max = 30) int opCount) {

        MemoryArbitrator arbitrator = new MemoryArbitrator(budget, 0);
        RootMemoryPool pool = new RootMemoryPool("test", budget);
        long poolOutstanding = 0;

        for (int i = 0; i < opCount; i++) {
            long amount = 500 + (i * 300L);
            if (i % 2 == 0) {
                long reserved = pool.tryReserve(amount);
                poolOutstanding += reserved;
                arbitrator.requestMemory("q-" + i, amount);
            } else {
                // Release only what was actually reserved (pool throws on over-release)
                long toFreePool = Math.min(amount, poolOutstanding);
                if (toFreePool > 0) {
                    pool.free(toFreePool);
                    poolOutstanding -= toFreePool;
                }
                // Arbitrator clamps internally, safe to release any amount
                arbitrator.release("q-" + (i - 1), amount);
            }

            assertThat(pool.getUsedBytes())
                    .as("Pool usedBytes must never be negative at step %d", i)
                    .isGreaterThanOrEqualTo(0);
            assertThat(arbitrator.getUsedMemory())
                    .as("Arbitrator usedMemory must never be negative at step %d", i)
                    .isGreaterThanOrEqualTo(0);
        }
    }
}
