package org.pangrova.resource.property.memory;

import org.pangrova.resource.memory.MemoryArbitrator;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 41: Resource quota enforcement.
 *
 * <p><b>Validates: Requirements 62.4</b></p>
 *
 * <p>For any sequence of memory allocation requests, the MemoryArbitrator
 * never allocates beyond its budget and correctly tracks per-query usage.</p>
 */
@Tag("Feature: architecture-remediation")
@Tag("Property 41: Resource quota enforcement")
class ResourceQuotaEnforcementPropertyTest {

    private static final long BUDGET = 1024 * 1024L;
    private static final long RESERVED = 64 * 1024L;

    @Property(tries = 10)
    @Label("Used memory never exceeds allocatable budget")
    void usedMemoryNeverExceedsBudget(
            @ForAll @IntRange(min = 1, max = 20) int queryCount,
            @ForAll @IntRange(min = 1024, max = 131072) int requestSize) {

        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, RESERVED);
        long allocatable = BUDGET - RESERVED;

        for (int i = 0; i < queryCount; i++) {
            arbitrator.requestMemory("q-" + i, requestSize);
        }

        assertThat(arbitrator.getUsedMemory())
                .as("Used memory must not exceed allocatable budget")
                .isLessThanOrEqualTo(allocatable);
    }

    @Property(tries = 10)
    @Label("Release restores available memory")
    void releaseRestoresAvailableMemory(
            @ForAll @IntRange(min = 1024, max = 65536) int allocSize) {

        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, RESERVED);

        long allocated = arbitrator.requestMemory("q1", allocSize);
        long usedAfterAlloc = arbitrator.getUsedMemory();

        arbitrator.release("q1", allocated);

        assertThat(arbitrator.getUsedMemory())
                .as("After full release, used memory must decrease by allocated amount")
                .isEqualTo(usedAfterAlloc - allocated);
    }

    @Property(tries = 10)
    @Label("Concurrent allocations stay within budget")
    void concurrentAllocationsStayWithinBudget(
            @ForAll @IntRange(min = 2, max = 8) int threadCount) throws InterruptedException {

        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, RESERVED);
        long allocatable = BUDGET - RESERVED;
        long perThread = allocatable / 2;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            threads[i] = new Thread(() ->
                    arbitrator.requestMemory("q-" + idx, perThread));
        }
        for (Thread t : threads) { t.start(); }
        for (Thread t : threads) { t.join(); }

        assertThat(arbitrator.getUsedMemory())
                .as("Concurrent allocations must not exceed budget")
                .isLessThanOrEqualTo(allocatable);
    }

    @Property(tries = 10)
    @Label("Zero or negative requests allocate nothing")
    void zeroOrNegativeRequestsAllocateNothing(
            @ForAll @IntRange(min = -1000, max = 0) int badSize) {

        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, RESERVED);
        long allocated = arbitrator.requestMemory("q1", badSize);

        assertThat(allocated).isZero();
        assertThat(arbitrator.getUsedMemory()).isZero();
    }

    @Property(tries = 10)
    @Label("Exhausted budget returns zero allocation")
    void exhaustedBudgetReturnsZero(
            @ForAll @IntRange(min = 1024, max = 65536) int extraRequest) {

        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, RESERVED);
        long allocatable = BUDGET - RESERVED;

        // Exhaust the budget
        arbitrator.requestMemory("q-fill", allocatable);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(allocatable);

        // Additional request should get zero
        long extra = arbitrator.requestMemory("q-extra", extraRequest);
        assertThat(extra).isZero();
    }
}
