package org.pangrova.coordinator.result;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.types.value.TypedRow;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.result.CoordinatorResultTestAccess.streamedRoot;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class CoordinatorResultBufferTest {
    private final RootAllocator allocator = new RootAllocator(16L * 1024 * 1024);
    private final CoordinatorResultWriter writer = new CoordinatorResultWriter(allocator);
    private final java.util.concurrent.ScheduledExecutorService expiration =
            Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void releaseFixture() throws InterruptedException {
        expiration.shutdownNow();
        assertThat(expiration.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        assertThat(allocator.getAllocatedMemory()).isZero();
        allocator.close();
    }

    @Test
    void oneReaderOwnsTheBatchAndQueryOperationThroughClose() {
        try (var buffer = new CoordinatorResultBuffer(expiration, 60_000)) {
            var exits = new AtomicInteger();
            var batch = batch(42);
            long bytes = batch.bytes();
            buffer.publish("query", batch, () -> {
                assertThat(allocator.getAllocatedMemory()).isZero();
                exits.incrementAndGet();
            });
            assertThat(buffer.bufferedBytes()).isEqualTo(bytes);
            try (var claim = buffer.claim("query")) {
                assertThat(streamedRoot(claim::streamTo).getVector("id").getObject(0)).isEqualTo(42);
                assertThat(buffer.claim("query")).isNull();
                buffer.discardUnopened("query");
                assertThat(buffer.bufferedBytes()).isEqualTo(bytes);
                assertThat(exits.get()).isZero();
            }
            assertThat(buffer.claim("query")).isNull();
            assertThat(buffer.size()).isZero();
            assertThat(buffer.bufferedBytes()).isZero();
            assertThat(exits.get()).isOne();
        }
    }

    @Test
    void expiresOnlyUnreadBatchesAndDoesNotReassignExpiredTickets() throws Exception {
        var blocker = new CountDownLatch(1);
        expiration.submit(() -> {
            blocker.await();
            return null;
        });
        try (var buffer = new CoordinatorResultBuffer(expiration, 20)) {
            var expired = new AtomicInteger();
            var reader = new AtomicInteger();
            buffer.publish("unopened", batch(1), expired::incrementAndGet);
            buffer.publish("opened", batch(2), reader::incrementAndGet);
            try (var claim = buffer.claim("opened")) {
                blocker.countDown();
                await().atMost(Duration.ofSeconds(3)).until(() -> expired.get() == 1);
                assertThat(buffer.claim("unopened")).isNull();
                assertThat(streamedRoot(claim::streamTo).getVector("id").getObject(0)).isEqualTo(2);
                assertThat(reader.get()).isZero();
            }
            assertThat(reader.get()).isOne();
        } finally {
            blocker.countDown();
        }
    }

    @Test
    void shutdownReleasesUnopenedBatchesAndWaitsForClaimedReaders() {
        var exits = new AtomicInteger();
        var buffer = new CoordinatorResultBuffer(expiration, 60_000);
        buffer.publish("unopened", batch(1), exits::incrementAndGet);
        buffer.publish("reading", batch(2), exits::incrementAndGet);
        try (var claim = buffer.claim("reading")) {
            buffer.seal();
            assertThat(exits.get()).isOne();
            assertThat(buffer.claim("unopened")).isNull();
            assertThatThrownBy(buffer::close).isInstanceOf(IllegalStateException.class);
            assertThat(streamedRoot(claim::streamTo).getVector("id").getObject(0)).isEqualTo(2);
            assertThatThrownBy(() -> buffer.publish("late", batch(3), exits::incrementAndGet))
                    .hasMessageContaining("closed");
        }
        buffer.close();
        assertThat(exits.get()).isEqualTo(3);
    }

    @Test
    void failedSchedulingAndDuplicateIdentityRetireOnlyTheLosingAllocation() {
        var exits = new AtomicInteger();
        try (var buffer = new CoordinatorResultBuffer(expiration, 60_000)) {
            var original = batch(1);
            buffer.publish("query", original, exits::incrementAndGet);
            assertThatThrownBy(() -> buffer.publish("query", batch(1), exits::incrementAndGet))
                    .hasMessageContaining("already owned");
            assertThatThrownBy(() -> buffer.publish("query", batch(2), exits::incrementAndGet))
                    .hasMessageContaining("already owned");
            expiration.shutdown();
            assertThatThrownBy(() -> buffer.publish("new", batch(3), exits::incrementAndGet))
                    .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
            assertThat(exits.get()).isEqualTo(3);
            assertThat(buffer.bufferedBytes()).isEqualTo(original.bytes());
            try (var claim = buffer.claim("query")) {
                assertThat(streamedRoot(claim::streamTo).getVector("id").getObject(0)).isEqualTo(1);
            }
            assertThat(exits.get()).isEqualTo(4);
        }
    }

    @Test
    void nullResultKeyDoesNotLeakItsBatchOrOperation() {
        var exits = new AtomicInteger();
        try (var buffer = new CoordinatorResultBuffer(expiration, 60_000)) {
            assertThatThrownBy(() -> buffer.publish(null, batch(1), exits::incrementAndGet))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> buffer.publish("missing", null, exits::incrementAndGet))
                    .isInstanceOf(NullPointerException.class);
            assertThat(exits.get()).isEqualTo(2);
            assertThat(buffer.bufferedBytes()).isZero();
        }
    }

    private CoordinatorResultBatch batch(int value) {
        return writer.buildBatch(List.of(TypedRow.builder().putInt("id", value).build()), List.of("id"));
    }
}
