package org.pangrova.coordinator.result;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.awaitility.Awaitility;
import org.pangrova.common.concurrent.ManagedExecutorFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatorResultBufferTest {

    private ScheduledExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void putAndGetReturnsBufferedBytes() {
        var buffer = new CoordinatorResultBuffer(executor, 10_000L);
        byte[] data = {1, 2, 3};

        buffer.put("q1", data);

        assertThat(buffer.get("q1")).isEqualTo(data);
        assertThat(buffer.contains("q1")).isTrue();
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void getReturnsNullForMissingQuery() {
        var buffer = new CoordinatorResultBuffer(executor, 10_000L);

        assertThat(buffer.get("missing")).isNull();
        assertThat(buffer.contains("missing")).isFalse();
    }

    @Test
    void removeDeletesEntry() {
        var buffer = new CoordinatorResultBuffer(executor, 10_000L);
        buffer.put("q1", new byte[]{1});

        buffer.remove("q1");

        assertThat(buffer.get("q1")).isNull();
        assertThat(buffer.size()).isEqualTo(0);
    }

    @Test
    void removeNonExistentIsNoOp() {
        var buffer = new CoordinatorResultBuffer(executor, 10_000L);
        buffer.remove("nonexistent");
        assertThat(buffer.size()).isEqualTo(0);
    }

    @Test
    void getRetainsDataUntilTtlExpires() {
        var buffer = new CoordinatorResultBuffer(executor, 10_000L);
        buffer.put("q1", new byte[]{1});

        // Multiple gets should all succeed before TTL
        assertThat(buffer.get("q1")).isNotNull();
        assertThat(buffer.get("q1")).isNotNull();
        assertThat(buffer.contains("q1")).isTrue();
    }

    @Test
    void ttlExpirationRemovesEntry() throws InterruptedException {
        var latch = new CountDownLatch(1);
        var shortTtlExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            var buffer = new CoordinatorResultBuffer(shortTtlExecutor, 50L);
            buffer.put("q1", new byte[]{1});

            // Schedule a check after TTL + margin
            shortTtlExecutor.schedule(latch::countDown, 150, TimeUnit.MILLISECONDS);
            latch.await(2, TimeUnit.SECONDS);

            assertThat(buffer.get("q1")).isNull();
            assertThat(buffer.size()).isEqualTo(0);
        } finally {
            shortTtlExecutor.shutdownNow();
        }
    }

    @Test
    void constructorRejectsNullExecutor() {
        assertThatThrownBy(() -> new CoordinatorResultBuffer(null, 1000L))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("expirationExecutor");
    }

    @Test
    void constructorRejectsNonPositiveRetention() {
        assertThatThrownBy(() -> new CoordinatorResultBuffer(executor, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CoordinatorResultBuffer(executor, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void putRejectsNullArguments() {
        var buffer = new CoordinatorResultBuffer(executor, 10_000L);

        assertThatThrownBy(() -> buffer.put(null, new byte[]{1}))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> buffer.put("q1", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void concurrentPutAndGetAreThreadSafe() throws InterruptedException {
        var buffer = new CoordinatorResultBuffer(executor, 60_000L);
        int threadCount = 8;
        var startLatch = new CountDownLatch(1);
        var doneLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final String queryId = "q" + i;
            final byte[] data = new byte[]{(byte) i};
            new Thread(() -> {
                try {
                    startLatch.await();
                    buffer.put(queryId, data);
                    assertThat(buffer.get(queryId)).isEqualTo(data);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(buffer.size()).isEqualTo(threadCount);
    }

    @Test
    void offHeapBytesTracksUsage() {
        var buffer = new CoordinatorResultBuffer(executor, 10_000L);
        assertThat(buffer.offHeapBytes()).isEqualTo(0);

        buffer.put("q1", new byte[]{1, 2, 3});
        assertThat(buffer.offHeapBytes()).isEqualTo(3);

        buffer.put("q2", new byte[]{4, 5});
        assertThat(buffer.offHeapBytes()).isEqualTo(5);

        buffer.remove("q1");
        assertThat(buffer.offHeapBytes()).isEqualTo(2);
    }

    @Test
    void closeCancelsOwnedExpirationAndSealsWrites() throws Exception {
        var buffer = new CoordinatorResultBuffer(executor, TimeUnit.HOURS.toMillis(1));
        buffer.put("retained-ack", new byte[]{1});

        buffer.close();
        executor.shutdown();

        assertThat(executor.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
        assertThat(buffer.size()).isZero();
        assertThat(buffer.offHeapBytes()).isZero();
        assertThatThrownBy(() -> buffer.put("late-ack", new byte[]{2}))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void expirationAdmissionFailureRollsBackBufferedOwnership() {
        var buffer = new CoordinatorResultBuffer(executor, 10_000L);
        executor.shutdown();

        assertThatThrownBy(() -> buffer.put("rejected-ack", new byte[]{1, 2}))
                .isInstanceOf(RejectedExecutionException.class);
        assertThat(buffer.get("rejected-ack")).isNull();
        assertThat(buffer.offHeapBytes()).isZero();
    }

    @Test
    void capacityRejectionCannotBecomeSuccessfulAcknowledgement() {
        var buffer = new CoordinatorResultBuffer(executor, 10_000L, 1, 2);
        buffer.put("retained-ack", new byte[]{1});

        assertThatThrownBy(() -> buffer.put("missing-ack", new byte[]{2}))
                .isInstanceOf(IllegalStateException.class);
        assertThat(buffer.get("retained-ack")).containsExactly((byte) 1);
        assertThat(buffer.get("missing-ack")).isNull();
        assertThat(buffer.offHeapBytes()).isEqualTo(1);
    }

    @Test
    void stoppedExpirationRetainsBytesUntilTransportOwnerCloses() throws Exception {
        var buffer = new CoordinatorResultBuffer(executor, TimeUnit.HOURS.toMillis(1));
        buffer.put("retained-ack", new byte[]{1, 2});

        buffer.stopExpirationScheduling();
        executor.shutdown();

        assertThat(executor.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
        assertThat(buffer.get("retained-ack")).containsExactly((byte) 1, (byte) 2);
        assertThatThrownBy(() -> buffer.put("late-ack", new byte[]{3}))
                .isInstanceOf(IllegalStateException.class);
        buffer.close();
        assertThat(buffer.size()).isZero();
        assertThat(buffer.offHeapBytes()).isZero();
    }

    @Test
    void alreadyEnteredExpirationCannotRemoveSealedAcknowledgement() throws Exception {
        var timer = (ScheduledThreadPoolExecutor) ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("entered-ack-expiration");
        var buffer = new CoordinatorResultBuffer(timer, 1);
        try {
            synchronized (buffer) {
                buffer.put("retained-ack", new byte[]{1});
                Awaitility.await("actual expiration callback entry")
                        .atMost(Duration.ofSeconds(2)).until(() -> timer.getActiveCount() == 1);
                buffer.stopExpirationScheduling();
            }
            timer.shutdown();
            assertThat(timer.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
            assertThat(buffer.get("retained-ack")).containsExactly((byte) 1);
        } finally {
            buffer.close();
            timer.shutdownNow();
            assertThat(timer.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        }
    }

}
