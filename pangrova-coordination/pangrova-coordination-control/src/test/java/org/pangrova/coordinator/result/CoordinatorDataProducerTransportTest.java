package org.pangrova.coordinator.result;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.transport.TransportServerConfig;
import org.pangrova.transport.flight.TransportServer;
import org.pangrova.types.value.TypedRow;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the production direct-columnar producer over actual TCP. */
class CoordinatorDataProducerTransportTest {
    @Test
    void servesOneBatchOnceAndReleasesBeforeEof() throws Exception {
        assertTransportRows(List.of(TypedRow.builder().putInt("ack", 42).build()), List.of("ack"));
    }

    @Test
    void emptyResultKeepsItsSchemaAndEndsAfterOneBatch() throws Exception {
        assertTransportRows(List.of(), List.of("ack"));
    }

    private static void assertTransportRows(List<TypedRow> rows, List<String> columns) throws Exception {
        var expiration = Executors.newSingleThreadScheduledExecutor();
        var reader = Executors.newSingleThreadExecutor();
        var barrier = new ReadBarrier();
        try (var allocator = new RootAllocator(16L * 1024 * 1024);
             var clientAllocator = new RootAllocator(16L * 1024 * 1024);
             var buffer = new CoordinatorResultBuffer(expiration, 60_000)) {
            var batch = new CoordinatorResultWriter(allocator).buildBatch(rows, columns);
            var exits = new AtomicInteger();
            buffer.publish("statement", batch, exits::incrementAndGet);
            var server = new TransportServer(new CoordinatorDataProducer(buffer),
                    new TransportServerConfig(0), allocator, List.of(barrier));
            try {
                server.start();
                var location = Location.forGrpcInsecure("localhost", server.getPort());
                try (var client = FlightClient.builder(clientAllocator, location).build()) {
                    var ticket = new Ticket("statement".getBytes(StandardCharsets.UTF_8));
                    var streamed = reader.submit(() -> {
                        try (var stream = client.getStream(ticket)) {
                            assertThat(stream.next()).isTrue();
                            assertThat(stream.getRoot().getRowCount()).isEqualTo(rows.size());
                            assertThat(stream.getRoot().getSchema().getFields()).extracting(field -> field.getName())
                                    .containsExactlyElementsOf(columns);
                            if (!rows.isEmpty()) {
                                assertThat(stream.getRoot().getVector("ack").getObject(0)).isEqualTo(42);
                            }
                            assertThat(stream.next()).isFalse();
                            return null;
                        }
                    });
                    try {
                        assertThat(barrier.sending.await(5, TimeUnit.SECONDS)).isTrue();
                        assertThat(exits.get()).isZero();
                        assertThat(buffer.bufferedBytes()).isEqualTo(batch.bytes());
                        assertThat(buffer.claim("statement")).isNull();
                    } finally {
                        barrier.resume.countDown();
                    }
                    streamed.get(5, TimeUnit.SECONDS);
                    assertThat(exits.get()).isOne();
                    assertThat(buffer.bufferedBytes()).isZero();
                    assertThat(allocator.getAllocatedMemory()).isZero();
                    assertThatThrownBy(() -> {
                        try (var second = client.getStream(ticket)) {
                            second.next();
                        }
                    }).isInstanceOf(org.apache.arrow.flight.FlightRuntimeException.class)
                            .hasMessageContaining("unavailable");
                }
            } finally {
                server.stop();
            }
        } finally {
            reader.shutdownNow();
            expiration.shutdownNow();
            assertThat(reader.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(expiration.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class ReadBarrier implements ServerInterceptor {
        private final CountDownLatch sending = new CountDownLatch(1);
        private final CountDownLatch resume = new CountDownLatch(1);

        @Override
        public <Request, Response> ServerCall.Listener<Request> interceptCall(
                ServerCall<Request, Response> request, Metadata headers, ServerCallHandler<Request, Response> next) {
            var forwarding = new ForwardingServerCall.SimpleForwardingServerCall<>(request) {
                private int messages;

                @Override
                public void sendMessage(Response message) {
                    if ("DoGet".equals(request.getMethodDescriptor().getBareMethodName()) && ++messages == 2) {
                        sending.countDown();
                        try {
                            if (!resume.await(8, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Flight test barrier did not resume");
                            }
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(failure);
                        }
                    }
                    super.sendMessage(message);
                }
            };
            return next.startCall(forwarding, headers);
        }
    }
}
