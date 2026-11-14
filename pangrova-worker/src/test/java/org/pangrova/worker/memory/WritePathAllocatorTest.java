package org.pangrova.worker.memory;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link WritePathAllocator}.
 * Task 202: validates dedicated write-path allocator bypasses Velox pool.
 */
class WritePathAllocatorTest {

    @Test
    void allocateWithinBudgetSucceeds() {
        var allocator = new WritePathAllocator(1024);
        long granted = allocator.allocate(512);
        assertThat(granted).isEqualTo(512);
        assertThat(allocator.usedBytes()).isEqualTo(512);
        assertThat(allocator.availableBytes()).isEqualTo(512);
    }

    @Test
    void allocateExceedingBudgetReturnsPartial() {
        var allocator = new WritePathAllocator(1024);
        allocator.allocate(800);
        long granted = allocator.allocate(500);
        // Should get only 224 (remaining)
        assertThat(granted).isEqualTo(224);
        assertThat(allocator.usedBytes()).isEqualTo(1024);
    }

    @Test
    void allocateWhenFullReturnsZero() {
        var allocator = new WritePathAllocator(1024);
        allocator.allocate(1024);
        assertThat(allocator.allocate(1)).isZero();
    }

    @Test
    void releaseFreesCapacity() {
        var allocator = new WritePathAllocator(1024);
        allocator.allocate(1024);
        allocator.release(512);
        assertThat(allocator.usedBytes()).isEqualTo(512);
        assertThat(allocator.availableBytes()).isEqualTo(512);
    }

    @Test
    void allocateZeroReturnsZero() {
        var allocator = new WritePathAllocator(1024);
        assertThat(allocator.allocate(0)).isZero();
        assertThat(allocator.allocate(-1)).isZero();
    }

    @Test
    void releaseZeroIsNoop() {
        var allocator = new WritePathAllocator(1024);
        allocator.allocate(100);
        allocator.release(0);
        allocator.release(-1);
        assertThat(allocator.usedBytes()).isEqualTo(100);
    }

    @Test
    void constructorRejectsNonPositiveBudget() {
        assertThatThrownBy(() -> new WritePathAllocator(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WritePathAllocator(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void tracksAllocationAndReleaseCounters() {
        var allocator = new WritePathAllocator(1024);
        allocator.allocate(100);
        allocator.allocate(200);
        allocator.release(150);
        assertThat(allocator.totalAllocations()).isEqualTo(2);
        assertThat(allocator.totalReleases()).isEqualTo(1);
    }

    @Test
    void concurrentAllocateReleaseIsThreadSafe() throws InterruptedException {
        var allocator = new WritePathAllocator(100_000);
        int threadCount = 8;
        int opsPerThread = 1000;
        var latch = new CountDownLatch(threadCount);
        var totalGranted = new AtomicLong(0);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        long granted = allocator.allocate(10);
                        if (granted > 0) {
                            totalGranted.addAndGet(granted);
                            allocator.release(granted);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        assertThat(allocator.usedBytes()).isZero();
        assertThat(totalGranted.get()).isPositive();
    }
}
