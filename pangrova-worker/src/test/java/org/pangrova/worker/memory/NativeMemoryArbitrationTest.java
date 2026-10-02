package org.pangrova.worker.memory;

import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.ShrinkableMemoryConsumer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that NativeMemoryCoordinator routes allocations through
 * MemoryArbitrator and that the arbitrator triggers spill callbacks
 * when memory pressure is high, preventing OS-level OOM.
 */
class NativeMemoryArbitrationTest {

    private static final long TOTAL_BUDGET = 1024 * 1024; // 1 MB total
    private static final long NATIVE_CAP = 768 * 1024;    // 768 KB native cap

    @Test
    void failedSpillKeepsPhysicalReservationCharged() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        var coordinator = new NativeMemoryCoordinator(
                NATIVE_CAP, arbitrator, new NativeSpillBridge(bytes -> 0));
        assertThat(coordinator.reserve(4096)).isEqualTo(4096);

        assertThat(coordinator.shrink(4096)).isZero();
        assertThat(coordinator.currentReservation()).isEqualTo(4096);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(4096);
    }

    @Test
    void successfulNativeReclaimUsesOnlyUnreserveCallbackForAccounting() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        AtomicReference<NativeMemoryCoordinator> callback = new AtomicReference<>();
        var bridge = new NativeSpillBridge(bytes -> {
            callback.get().unreserve(1024);
            return 1024;
        });
        var coordinator = new NativeMemoryCoordinator(NATIVE_CAP, arbitrator, bridge);
        callback.set(coordinator);
        assertThat(coordinator.reserve(4096)).isEqualTo(4096);

        assertThat(coordinator.shrink(4096)).isEqualTo(1024);
        assertThat(coordinator.currentReservation()).isEqualTo(3072);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(3072);
    }

    @Test
    void signalWithoutNativeUnreserveNeverFreesArbitratorCredit() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        var coordinator = new NativeMemoryCoordinator(
                NATIVE_CAP, arbitrator, new NativeSpillBridge(bytes -> 1));
        assertThat(coordinator.reserve(4096)).isEqualTo(4096);

        assertThat(coordinator.shrink(4096)).isZero();
        assertThat(coordinator.currentReservation()).isEqualTo(4096);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(4096);
    }

    @Test
    void reserveRoutedThroughArbitrator() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        var coordinator = new NativeMemoryCoordinator(NATIVE_CAP, arbitrator);

        long granted = coordinator.reserve(1024);

        assertThat(granted).isEqualTo(1024);
        assertThat(coordinator.currentReservation()).isEqualTo(1024);
        // Arbitrator should reflect the allocation
        assertThat(arbitrator.getUsedMemory()).isEqualTo(1024);
    }

    @Test
    void unreserveReleasesFromArbitrator() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        var coordinator = new NativeMemoryCoordinator(NATIVE_CAP, arbitrator);

        coordinator.reserve(2048);
        coordinator.unreserve(1024);

        assertThat(coordinator.currentReservation()).isEqualTo(1024);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(1024);
    }

    @Test
    void globalCapPreventsAllocationWhenBudgetExhausted() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        var coordinator = new NativeMemoryCoordinator(NATIVE_CAP, arbitrator);

        // Exhaust the global budget via direct arbitrator allocation (simulating Java heap usage)
        long javaAllocated = arbitrator.requestMemory("java-query-1", TOTAL_BUDGET);
        assertThat(javaAllocated).isEqualTo(TOTAL_BUDGET);

        // Native allocation should be denied — global budget exhausted
        long nativeGranted = coordinator.reserve(1024);
        assertThat(nativeGranted).isEqualTo(0);
    }

    @Test
    void nativeCapPreventsAllocationBeyondLocalLimit() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        var coordinator = new NativeMemoryCoordinator(NATIVE_CAP, arbitrator);

        // Fill native cap
        long granted = coordinator.reserve(NATIVE_CAP);
        assertThat(granted).isEqualTo(NATIVE_CAP);

        // Further native allocation denied by local cap even though global has room
        long excess = coordinator.reserve(1);
        assertThat(excess).isEqualTo(0);
        assertThat(arbitrator.getAvailableMemory()).isGreaterThan(0);
    }

    @Test
    void arbitratorTriggersSpillWhenNearLimit() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);

        // Register a spillable consumer that can release memory
        var spillableBytes = new AtomicLong(512 * 1024); // 512 KB reclaimable
        var spillTriggered = new AtomicLong(0);
        ShrinkableMemoryConsumer spillConsumer = new ShrinkableMemoryConsumer() {
            @Override
            public String consumerId() {
                return "test-spill-consumer";
            }

            @Override
            public long currentUsageBytes() {
                return spillableBytes.get();
            }

            @Override
            public long shrink(long targetBytes) {
                long toRelease = Math.min(targetBytes, spillableBytes.get());
                spillableBytes.addAndGet(-toRelease);
                spillTriggered.addAndGet(toRelease);
                // Release back to arbitrator (simulating actual memory freed)
                arbitrator.releaseReservation(toRelease);
                return toRelease;
            }
        };
        arbitrator.registerConsumer(spillConsumer);

        // Pre-allocate the spillable consumer's memory in the arbitrator
        long preAllocated = arbitrator.reserve(512 * 1024);
        assertThat(preAllocated).isEqualTo(512 * 1024);

        // Fill remaining budget
        long remaining = TOTAL_BUDGET - 512 * 1024;
        long filled = arbitrator.reserve(remaining);
        assertThat(filled).isEqualTo(remaining);
        assertThat(arbitrator.getAvailableMemory()).isEqualTo(0);

        // Now attempt a new allocation — should trigger spill and succeed
        long newAllocation = arbitrator.reserve(256 * 1024);
        assertThat(newAllocation).isEqualTo(256 * 1024);
        assertThat(spillTriggered.get()).isGreaterThanOrEqualTo(256 * 1024);
    }

    @Test
    void globalMemoryCapJavaHeapPlusNativeDoesNotExceedTotal() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        var coordinator = new NativeMemoryCoordinator(NATIVE_CAP, arbitrator);

        // Simulate Java heap consuming 800 KB
        long javaUsed = arbitrator.requestMemory("java-query-1", 800 * 1024);
        assertThat(javaUsed).isEqualTo(800 * 1024);

        // Native can only get remaining (1024 - 800 = 224 KB), not full native cap
        long nativeGranted = coordinator.reserve(NATIVE_CAP);
        // Should get at most what's available globally (224 KB)
        assertThat(nativeGranted).isEqualTo(TOTAL_BUDGET - 800 * 1024);

        // Total used must not exceed total budget
        assertThat(arbitrator.getUsedMemory()).isLessThanOrEqualTo(TOTAL_BUDGET);
    }

    @Test
    void spillTriggeredWhenNativeAllocationNearGlobalLimit() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        var coordinator = new NativeMemoryCoordinator(NATIVE_CAP, arbitrator);

        // Register a Java-side spillable consumer
        var javaBufferBytes = new AtomicLong(500 * 1024);
        var spillCount = new AtomicLong(0);
        ShrinkableMemoryConsumer javaBuffer = new ShrinkableMemoryConsumer() {
            @Override
            public String consumerId() {
                return "java-scan-buffer";
            }

            @Override
            public long currentUsageBytes() {
                return javaBufferBytes.get();
            }

            @Override
            public long shrink(long targetBytes) {
                long toRelease = Math.min(targetBytes, javaBufferBytes.get());
                javaBufferBytes.addAndGet(-toRelease);
                spillCount.incrementAndGet();
                arbitrator.releaseReservation(toRelease);
                return toRelease;
            }
        };
        arbitrator.registerConsumer(javaBuffer);

        // Pre-allocate the Java buffer's memory in the arbitrator
        arbitrator.reserve(500 * 1024);

        // Fill most of the remaining budget via native (500KB native < 768KB cap)
        long nativeFirst = coordinator.reserve(500 * 1024);
        assertThat(nativeFirst).isEqualTo(500 * 1024);

        // Budget is now full (500 KB java + 500 KB native = 1000 KB, 24 KB remaining)
        // Request 60 KB native — fits local cap (500+60=560 < 768) but exceeds global budget
        // This triggers spill of java buffer, then allocation succeeds
        long nativeSecond = coordinator.reserve(60 * 1024);
        assertThat(nativeSecond).isEqualTo(60 * 1024);
        assertThat(spillCount.get()).isGreaterThan(0);

        // Total never exceeds budget
        assertThat(arbitrator.getUsedMemory()).isLessThanOrEqualTo(TOTAL_BUDGET);
    }

    @Test
    void noOomWhenAllConsumersCooperate() {
        var arbitrator = new MemoryArbitrator(TOTAL_BUDGET, 0);
        var coordinator = new NativeMemoryCoordinator(NATIVE_CAP, arbitrator);

        // Register coordinator as consumer (already done in constructor)
        // Register another spillable consumer
        var bufferUsage = new AtomicLong(0);
        ShrinkableMemoryConsumer buffer = new ShrinkableMemoryConsumer() {
            @Override
            public String consumerId() {
                return "cooperative-buffer";
            }

            @Override
            public long currentUsageBytes() {
                return bufferUsage.get();
            }

            @Override
            public long shrink(long targetBytes) {
                long toRelease = Math.min(targetBytes, bufferUsage.get());
                bufferUsage.addAndGet(-toRelease);
                arbitrator.releaseReservation(toRelease);
                return toRelease;
            }
        };
        arbitrator.registerConsumer(buffer);

        // Allocate and release in cycles — simulating workload
        for (int i = 0; i < 100; i++) {
            long chunk = 64 * 1024; // 64 KB per iteration

            // Simulate Java buffer growth
            long bufAlloc = arbitrator.reserve(chunk);
            if (bufAlloc > 0) {
                bufferUsage.addAndGet(bufAlloc);
            }

            // Simulate native allocation
            long nativeAlloc = coordinator.reserve(chunk);

            // Release some native
            if (nativeAlloc > 0) {
                coordinator.unreserve(nativeAlloc / 2);
            }
        }

        // Invariant: used memory never exceeds budget
        assertThat(arbitrator.getUsedMemory()).isLessThanOrEqualTo(TOTAL_BUDGET);
        assertThat(coordinator.currentReservation()).isLessThanOrEqualTo(NATIVE_CAP);
    }
}
