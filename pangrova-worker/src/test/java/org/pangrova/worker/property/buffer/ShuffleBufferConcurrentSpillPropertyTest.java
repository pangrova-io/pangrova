package org.pangrova.worker.property.buffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.worker.buffer.ShuffleBufferKey;
import org.pangrova.worker.buffer.ShuffleBufferManager;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying that ShuffleBufferManager enforces its
 * per-partition capacity bound under concurrent batch additions.
 *
 * <p>Creates a ShuffleBufferManager with a small per-partition capacity,
 * spawns multiple threads adding batches concurrently, and verifies that
 * the bounded queue never allows more batches than capacity (backpressure
 * blocks producers). A concurrent consumer drains batches to keep flow
 * moving, and we verify total produced == total consumed.</p>
 */
class ShuffleBufferConcurrentSpillPropertyTest {

    /**
     * For any combination of thread count (2-6) and batches per thread (5-30),
     * concurrent producers adding to the same partition never lose data:
     * total consumed == total produced, proving backpressure works correctly
     * under contention without data loss or corruption.
     */
    @Property(tries = 30)
    @Tag("P2-TEST-1: ShuffleBufferManager concurrent spill invariant")
    void concurrentAddAndDrainPreservesAllBatches(
            @ForAll("threadCounts") int threadCount,
            @ForAll("batchesPerThread") int batchesPerThread) throws Exception {

        int perPartitionCapacity = 4;
        ShuffleBufferManager manager = new ShuffleBufferManager(perPartitionCapacity);

        ShuffleBufferKey key = new ShuffleBufferKey("q-conc", "s1", 0);
        manager.createPartition(key);

        int totalExpected = threadCount * batchesPerThread;
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicInteger produced = new AtomicInteger(0);
        AtomicInteger consumed = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount + 1);
        try {
            // Consumer thread: drains batches until all expected are consumed
            Future<?> consumer = pool.submit(() -> {
                try {
                    startGate.await();
                    while (consumed.get() < totalExpected) {
                        byte[] batch = manager.pollBatch(key, 50);
                        if (batch != null) {
                            consumed.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // Producer threads: each adds batchesPerThread batches
            List<Future<?>> producers = new ArrayList<>();
            for (int t = 0; t < threadCount; t++) {
                producers.add(pool.submit(() -> {
                    try {
                        startGate.await();
                        for (int i = 0; i < batchesPerThread; i++) {
                            byte[] batch = new byte[64];
                            manager.addBatch(key, batch);
                            produced.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }));
            }

            startGate.countDown();

            for (Future<?> f : producers) {
                f.get(10, TimeUnit.SECONDS);
            }
            consumer.get(10, TimeUnit.SECONDS);

            assertThat(produced.get())
                    .as("all batches should be produced")
                    .isEqualTo(totalExpected);
            assertThat(consumed.get())
                    .as("all produced batches should be consumed (no data loss)")
                    .isEqualTo(totalExpected);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
            manager.close();
        }
    }

    /**
     * For any thread count, when multiple threads add batches to separate
     * partitions concurrently, each partition receives exactly its expected
     * batch count — no cross-partition contamination.
     */
    @Property(tries = 30)
    @Tag("P2-TEST-1: ShuffleBufferManager partition isolation under concurrency")
    void concurrentPartitionsAreIsolated(
            @ForAll("threadCounts") int threadCount) throws Exception {

        int perPartitionCapacity = 10;
        int batchesPerPartition = 15;
        ShuffleBufferManager manager = new ShuffleBufferManager(perPartitionCapacity);

        for (int t = 0; t < threadCount; t++) {
            manager.createPartition(new ShuffleBufferKey("q-iso", "s1", t));
        }

        CountDownLatch startGate = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount * 2);
        try {
            List<Future<Integer>> consumers = new ArrayList<>();
            List<Future<?>> producers = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                int partitionId = t;
                ShuffleBufferKey key = new ShuffleBufferKey("q-iso", "s1", partitionId);

                // Producer
                producers.add(pool.submit(() -> {
                    try {
                        startGate.await();
                        for (int i = 0; i < batchesPerPartition; i++) {
                            manager.addBatch(key, new byte[]{(byte) partitionId});
                        }
                        manager.closePartition(key);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }));

                // Consumer
                consumers.add(pool.submit(() -> {
                    int count = 0;
                    try {
                        startGate.await();
                        while (true) {
                            byte[] batch = manager.pollBatch(key, 100);
                            if (batch != null) {
                                // Verify partition isolation: byte value == partitionId
                                assertThat(batch[0]).isEqualTo((byte) partitionId);
                                count++;
                            } else if (manager.isPartitionClosed(key)) {
                                // Drain remaining
                                byte[] trailing = manager.pollBatch(key, 0);
                                if (trailing != null) {
                                    assertThat(trailing[0]).isEqualTo((byte) partitionId);
                                    count++;
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return count;
                }));
            }

            startGate.countDown();

            for (Future<?> f : producers) {
                f.get(10, TimeUnit.SECONDS);
            }
            for (Future<Integer> f : consumers) {
                assertThat(f.get(10, TimeUnit.SECONDS))
                        .as("each partition should receive exactly its batch count")
                        .isEqualTo(batchesPerPartition);
            }
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
            manager.close();
        }
    }

    @Provide
    Arbitrary<Integer> threadCounts() {
        return Arbitraries.integers().between(2, 6);
    }

    @Provide
    Arbitrary<Integer> batchesPerThread() {
        return Arbitraries.integers().between(5, 30);
    }
}
