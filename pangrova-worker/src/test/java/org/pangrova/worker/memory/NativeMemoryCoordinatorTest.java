package org.pangrova.worker.memory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.NodeMemoryPool;
import org.pangrova.resource.memory.RootMemoryPool;
import org.pangrova.worker.bootstrap.phase.ShuffleServiceMemoryLease;

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
    void actualNativeGrowthChargesNodeAndGlobalBudgetsExactlyOnce() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(1024, 0);
        NodeMemoryPool node = new RootMemoryPool("root-native", 768)
                .createNodePool("node-native", 768);
        var coordinator = new NativeMemoryCoordinator(1024, arbitrator, node, null);

        assertEquals(512, coordinator.reserve(512));
        assertEquals(512, node.getUsedBytes());
        assertEquals(512, arbitrator.getUsedMemory());
        assertEquals(0, coordinator.reserve(300));
        assertEquals(512, node.getUsedBytes());
        assertEquals(512, arbitrator.getUsedMemory());

        coordinator.unreserve(512);
        assertEquals(0, node.getUsedBytes());
        assertEquals(0, arbitrator.getUsedMemory());
    }

    @Test
    void concurrentNativeGrowthAndReleaseLeaveBothLedgersBalanced() throws Exception {
        MemoryArbitrator arbitrator = new MemoryArbitrator(1024, 0);
        NodeMemoryPool node = new RootMemoryPool("root-concurrent-native", 768)
                .createNodePool("node-concurrent-native", 768);
        var coordinator = new NativeMemoryCoordinator(768, arbitrator, node, null);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            var tasks = new java.util.ArrayList<Future<?>>();
            for (int thread = 0; thread < 8; thread++) {
                tasks.add(pool.submit(() -> {
                    for (int iteration = 0; iteration < 500; iteration++) {
                        long granted = coordinator.reserve(32);
                        if (granted > 0) {
                            coordinator.unreserve(granted);
                        }
                    }
                }));
            }
            for (Future<?> task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(0, coordinator.currentReservation());
        assertEquals(0, node.getUsedBytes());
        assertEquals(0, arbitrator.getUsedMemory());
        assertEquals(0, node.getDoubleReleaseCount());
    }

    @Test
    void resetBudgetReleasesOnlyNativeCallbackReservation() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(MAX_BYTES, 0);
        RootMemoryPool root = new RootMemoryPool("root", MAX_BYTES);
        NodeMemoryPool serviceNode = root.createNodePool("service-node", MAX_BYTES / 2);
        NodeMemoryPool taskNode = root.createNodePool("task-node", MAX_BYTES / 2);
        try (ShuffleServiceMemoryLease service = ShuffleServiceMemoryLease.reserve(
                "worker", 100, 200, serviceNode, arbitrator)) {
            assertEquals(150, taskNode.tryReserve(150));
            assertEquals(150, arbitrator.reserve(taskNode, 150));
            var coordinator = new NativeMemoryCoordinator(MAX_BYTES, arbitrator);
            assertEquals(80, coordinator.reserve(80));
            assertEquals(530, arbitrator.getUsedMemory());

            coordinator.resetBudget();

            assertEquals(0, coordinator.currentReservation());
            assertEquals(450, arbitrator.getUsedMemory());
            assertEquals(300, serviceNode.getUsedBytes());
            assertEquals(150, taskNode.getUsedBytes());
            assertEquals(300, arbitrator.snapshot().get(serviceNode.getName()));
            assertEquals(150, arbitrator.snapshot().get(taskNode.getName()));
            arbitrator.release(taskNode, 150);
            taskNode.free(150);
        }
        assertEquals(0, arbitrator.getUsedMemory());
    }

    @Test
    void concurrentReserveCannotCrossLocalCapWhileFirstRequestIsInArbitration() throws Exception {
        CountDownLatch firstEnteredArbitrator = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        MemoryArbitrator arbitrator = new MemoryArbitrator(1000, 0) {
            @Override
            public long reserve(long bytes) {
                if (first.compareAndSet(true, false)) {
                    firstEnteredArbitrator.countDown();
                    try {
                        if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("first arbitration did not resume");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
                return super.reserve(bytes);
            }
        };
        var coordinator = new NativeMemoryCoordinator(100, arbitrator);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Long> firstGrant = pool.submit(() -> coordinator.reserve(70));
            assertTrue(firstEnteredArbitrator.await(5, TimeUnit.SECONDS));
            Future<Long> secondGrant = pool.submit(() -> coordinator.reserve(70));
            assertEquals(0, secondGrant.get(5, TimeUnit.SECONDS));
            releaseFirst.countDown();
            assertEquals(70, firstGrant.get(5, TimeUnit.SECONDS));
            assertEquals(70, coordinator.currentReservation());
            assertEquals(70, arbitrator.getUsedMemory());
        } finally {
            releaseFirst.countDown();
            pool.shutdownNow();
        }
    }

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
