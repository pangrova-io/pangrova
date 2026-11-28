package org.pangrova.coordinator.result;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.types.value.TypedRow;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatorControlBufferAdmissionTest {
    @Test
    void resultCapacityRejectsPublicationAndReleasesTheLosingOperation() throws Exception {
        var expiration = Executors.newSingleThreadScheduledExecutor();
        try (var allocator = new RootAllocator(16L * 1024 * 1024);
             var buffer = new CoordinatorResultBuffer(expiration, 60_000, 1, Long.MAX_VALUE)) {
            var writer = new CoordinatorResultWriter(allocator);
            var row = TypedRow.builder().putInt("id", 1).build();
            var exits = new AtomicInteger();
            var service = new CoordinatorResultService(buffer, writer,
                    new ResultEndpoint("localhost", 9091, "unused", "grpc"), ignored -> exits::incrementAndGet);
            service.dispatchControlPlaneRows("existing", List.of(row), List.of("id"));
            assertThatThrownBy(() -> service.dispatchControlPlaneRows("rejected", List.of(row), List.of("id")))
                    .hasMessageContaining("capacity");
            assertThat(exits.get()).isOne();
            assertThat(buffer.claim("rejected")).isNull();
            assertThat(buffer.size()).isOne();
            assertThat(allocator.getAllocatedMemory()).isEqualTo(buffer.bufferedBytes());
        } finally {
            expiration.shutdownNow();
            assertThat(expiration.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void concurrentWritersCannotOversubscribeTheArrowByteBudget() throws Exception {
        var expiration = Executors.newSingleThreadScheduledExecutor();
        var writers = Executors.newFixedThreadPool(8);
        try (var allocator = new RootAllocator(16L * 1024 * 1024)) {
            var writer = new CoordinatorResultWriter(allocator);
            var rows = List.of(TypedRow.builder().putInt("id", 1).build());
            long bytes;
            try (var sample = writer.buildBatch(rows, List.of("id"))) {
                bytes = sample.bytes();
            }
            try (var buffer = new CoordinatorResultBuffer(expiration, 60_000, 8, bytes * 2)) {
                var start = new CountDownLatch(1);
                var rejected = new AtomicInteger();
                var exited = new AtomicInteger();
                var requests = java.util.stream.IntStream.range(0, 8).mapToObj(index -> writers.submit(() -> {
                    start.await();
                    try {
                        buffer.publish("request-" + index, writer.buildBatch(rows, List.of("id")),
                                exited::incrementAndGet);
                    } catch (IllegalStateException failure) {
                        assertThat(failure).hasMessageContaining("capacity");
                        rejected.incrementAndGet();
                    }
                    return null;
                })).toList();
                start.countDown();
                for (var request : requests) request.get(5, TimeUnit.SECONDS);
                assertThat(buffer.size()).isEqualTo(2);
                assertThat(buffer.bufferedBytes()).isEqualTo(bytes * 2);
                assertThat(rejected.get()).isEqualTo(6);
                assertThat(exited.get()).isEqualTo(6);
                assertThat(allocator.getAllocatedMemory()).isEqualTo(bytes * 2);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        } finally {
            writers.shutdownNow();
            expiration.shutdownNow();
            assertThat(writers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(expiration.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
