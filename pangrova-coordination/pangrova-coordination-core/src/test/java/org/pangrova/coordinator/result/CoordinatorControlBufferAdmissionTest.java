package org.pangrova.coordinator.result;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.pangrova.client.core.protocol.ResultEndpoint;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatorControlBufferAdmissionTest {
    @Test
    void quotaFailureCannotPublishACompletedAcknowledgement() throws Exception {
        var scheduler = new ScheduledThreadPoolExecutor(1);
        try (var allocator = new RootAllocator(1024 * 1024);
             var buffer = new CoordinatorResultBuffer(scheduler, 60_000, 1, 1)) {
            buffer.put("existing", new byte[] {1});
            var service = new CoordinatorResultService(buffer, new CoordinatorResultWriter(allocator),
                    new ResultEndpoint("localhost", 0, "", "grpc"));
            var finished = new AtomicBoolean();
            assertThatThrownBy(() -> service.dispatchControlPlaneRows("new-ack",
                    List.of(TypedRow.of(Map.of("status", new ColumnValue.StringValue("OK")))),
                    List.of("status"), () -> finished.set(true)))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(finished).isFalse();
            assertThat(buffer.contains("new-ack")).isFalse();
            assertThat(buffer.size()).isEqualTo(1);
            assertThat(allocator.getAllocatedMemory()).isZero();
        } finally {
            scheduler.shutdownNow();
            assertThat(scheduler.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void duplicateIdentityIsExactEvenWhenCapacityIsFull() throws Exception {
        var scheduler = new ScheduledThreadPoolExecutor(1);
        try (var buffer = new CoordinatorResultBuffer(scheduler, 60_000, 1, 3)) {
            buffer.put("same", new byte[] {1, 2, 3});
            buffer.put("same", new byte[] {1, 2, 3});
            assertThat(buffer.size()).isEqualTo(1);
            assertThatThrownBy(() -> buffer.put("same", new byte[] {9, 8, 7}))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(buffer.get("same")).containsExactly(1, 2, 3);
            assertThatThrownBy(() -> buffer.put("other", new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(buffer.size()).isEqualTo(1);
        } finally {
            scheduler.shutdownNow();
            assertThat(scheduler.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void realSchedulerRejectionRollsBackTheSamePublishedEntry() throws Exception {
        var scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.shutdown();
        assertThat(scheduler.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        try (var buffer = new CoordinatorResultBuffer(scheduler, 60_000, 1, 3)) {
            assertThatThrownBy(() -> buffer.put("rejected", new byte[] {1, 2, 3}))
                    .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
            assertThat(buffer.contains("rejected")).isFalse();
            assertThat(buffer.size()).isZero();
        }
    }

    @Test
    void closeCancelsOwnedExpiryAndSealsFurtherPublication() throws Exception {
        var scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setRemoveOnCancelPolicy(true);
        try (var buffer = new CoordinatorResultBuffer(scheduler, 60_000, 1, 3)) {
            buffer.put("existing", new byte[] {1, 2, 3});
            var actualExpiry = (Future<?>) scheduler.getQueue().peek();
            assertThat(actualExpiry).isNotNull();
            buffer.close();
            assertThat(actualExpiry.isCancelled()).isTrue();
            assertThat(scheduler.getQueue()).isEmpty();
            assertThat(buffer.size()).isZero();
            assertThatThrownBy(() -> buffer.put("after-close", new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(buffer.size()).isZero();
        } finally {
            scheduler.shutdownNow();
            assertThat(scheduler.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void realConcurrentPublishersCannotOverspendOneBudget() throws Exception {
        var scheduler = new ScheduledThreadPoolExecutor(1);
        var writers = Executors.newFixedThreadPool(8);
        try (var buffer = new CoordinatorResultBuffer(scheduler, 60_000, 8, 10)) {
            var ready = new CountDownLatch(8);
            var start = new CountDownLatch(1);
            List<Future<Boolean>> attempts = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                String key = "owner-" + i;
                attempts.add(writers.submit(() -> {
                    ready.countDown();
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    try {
                        buffer.put(key, new byte[6]);
                        return true;
                    } catch (IllegalStateException rejected) {
                        return false;
                    }
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            int accepted = 0;
            for (var attempt : attempts) {
                if (attempt.get(10, TimeUnit.SECONDS)) accepted++;
            }
            assertThat(accepted).isEqualTo(1);
            assertThat(buffer.size()).isEqualTo(1);
        } finally {
            writers.shutdownNow();
            scheduler.shutdownNow();
            assertThat(writers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(scheduler.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
