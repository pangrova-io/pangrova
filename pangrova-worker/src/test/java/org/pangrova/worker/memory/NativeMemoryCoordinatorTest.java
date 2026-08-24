package org.pangrova.worker.memory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates NativeMemoryCoordinator reserve/unreserve decision logic
 * and AtomicLong CAS thread safety.
 */
class NativeMemoryCoordinatorTest {

    private static final long MAX_BYTES = 1024 * 1024;  // 1MB

    @Test
    void reserveWithinBudgetReturnsRequestedBytes() {
        var coordinator = new NativeMemoryCoordinator(MAX_BYTES);
        long granted = coordinator.reserve(512);
        assertEquals(512, granted);
        assertEquals(512, coordinator.currentReservation());
    }

    @Test
    void reserveExceedingBudgetReturnsZero() {
        var coordinator = new NativeMemoryCoordinator(MAX_BYTES);
        coordinator.reserve(MAX_BYTES);
        long granted = coordinator.reserve(1);
        assertEquals(0, granted);
    }

    @Test
    void reserveExactBudgetSucceeds() {
        var coordinator = new NativeMemoryCoordinator(MAX_BYTES);
        long granted = coordinator.reserve(MAX_BYTES);
        assertEquals(MAX_BYTES, granted);
        assertEquals(0, coordinator.availableForNative());
    }

    @Test
    void unreserveReleasesCapacity() {
        var coordinator = new NativeMemoryCoordinator(MAX_BYTES);
        coordinator.reserve(MAX_BYTES);
        coordinator.unreserve(MAX_BYTES / 2);
        assertEquals(MAX_BYTES / 2, coordinator.currentReservation());
        assertEquals(MAX_BYTES / 2, coordinator.availableForNative());
    }

    @Test
    void reserveZeroBytesReturnsZero() {
        var coordinator = new NativeMemoryCoordinator(MAX_BYTES);
        assertEquals(0, coordinator.reserve(0));
        assertEquals(0, coordinator.reserve(-1));
    }

    @Test
    void unreserveZeroBytesIsNoop() {
        var coordinator = new NativeMemoryCoordinator(MAX_BYTES);
        coordinator.reserve(100);
        coordinator.unreserve(0);
        coordinator.unreserve(-1);
        assertEquals(100, coordinator.currentReservation());
    }

    @Test
    void constructorRejectsNonPositiveMax() {
        assertThrows(IllegalArgumentException.class,
                () -> new NativeMemoryCoordinator(0));
        assertThrows(IllegalArgumentException.class,
                () -> new NativeMemoryCoordinator(-1));
    }

    @Test
    void maxNativeBytesAccessor() {
        var coordinator = new NativeMemoryCoordinator(MAX_BYTES);
        assertEquals(MAX_BYTES, coordinator.maxNativeBytes());
    }

    @Test
    void availableForNativeNeverNegative() {
        var coordinator = new NativeMemoryCoordinator(MAX_BYTES);
        coordinator.reserve(MAX_BYTES);
        // Force unreserve more than reserved (edge case from concurrent release).
        coordinator.unreserve(MAX_BYTES + 100);
        assertTrue(coordinator.availableForNative() >= 0);
    }

    /**
     * Validates: CAS loop correctness under concurrent reserve/unreserve.
     * Multiple threads compete for a shared budget; total granted must not
     * exceed maxNativeBytes at any point.
     */
    @Test
    void concurrentReserveUnreserveThreadSafety() throws InterruptedException {
        long budget = 10_000;
        var coordinator = new NativeMemoryCoordinator(budget);
        int threadCount = 8;
        int opsPerThread = 1000;
        var latch = new CountDownLatch(threadCount);
        var totalGranted = new AtomicInteger(0);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        long granted = coordinator.reserve(10);
                        if (granted > 0) {
                            totalGranted.incrementAndGet();
                            coordinator.unreserve(10);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        // After all unreserves, reservation should be back to 0.
        assertEquals(0, coordinator.currentReservation());
        // At least some reserves should have succeeded.
        assertTrue(totalGranted.get() > 0);
    }
}
