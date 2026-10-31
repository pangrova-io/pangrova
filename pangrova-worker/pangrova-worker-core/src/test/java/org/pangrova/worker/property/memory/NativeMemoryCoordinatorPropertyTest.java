package org.pangrova.worker.property.memory;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.worker.memory.NativeMemoryCoordinator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link NativeMemoryCoordinator} CAS loop under concurrency.
 */
class NativeMemoryCoordinatorPropertyTest {

    @Property(tries = 10)
    void concurrentReserveNeverExceedsBudget(
            @ForAll @LongRange(min = 1, max = 100) long numThreads,
            @ForAll @LongRange(min = 1024, max = 1024 * 1024) long budget) throws Exception {
        int threads = (int) numThreads;
        NativeMemoryCoordinator coordinator = new NativeMemoryCoordinator(budget);
        long perThread = Math.max(1, budget / threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong totalGranted = new AtomicLong(0);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try { latch.await(); } catch (InterruptedException e) { return; }
                long granted = coordinator.reserve(perThread);
                totalGranted.addAndGet(granted);
            });
        }
        latch.countDown();
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(coordinator.currentReservation()).isLessThanOrEqualTo(budget);
        assertThat(totalGranted.get()).isLessThanOrEqualTo(budget);
    }

    @Property(tries = 10)
    void reserveAndUnreserveNetZero(
            @ForAll @LongRange(min = 1, max = 1000) long amount) {
        NativeMemoryCoordinator coordinator = new NativeMemoryCoordinator(10_000);
        long granted = coordinator.reserve(amount);
        if (granted > 0) {
            coordinator.unreserve(granted);
        }
        assertThat(coordinator.currentReservation()).isEqualTo(0);
    }

    @Property(tries = 10)
    void unreserveBeyondReservedClampsToZero(
            @ForAll @LongRange(min = 1, max = 1000) long amount) {
        NativeMemoryCoordinator coordinator = new NativeMemoryCoordinator(10_000);
        coordinator.unreserve(amount);
        assertThat(coordinator.currentReservation()).isGreaterThanOrEqualTo(0);
    }
}
