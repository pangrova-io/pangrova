/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.transport;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.ResultProtocol;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class FlightResultFetcherDeadlineTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void liveFlightStreamCannotOutrunCallerQueryDeadline(boolean streaming) throws Exception {
        try (RootAllocator allocator = new RootAllocator(32L * 1024 * 1024)) {
            var producer = new DeadlineProducer(allocator);
            verifyDeadlineAndRecovery(allocator, producer, streaming);
            assertThat(allocator.getAllocatedMemory()).isZero();
            assertThat(allocator.getChildAllocators()).isEmpty();
        }
    }

    private static void verifyDeadlineAndRecovery(
            RootAllocator allocator, DeadlineProducer producer, boolean streaming) throws Exception {
        try (FlightServer server = FlightServer.builder(
                     allocator, Location.forGrpcInsecure("127.0.0.1", 0),
                     producer).build().start();
             FlightResultFetcher fetcher = new FlightResultFetcher()) {
            var executor = ManagedExecutorFactory.newSingleThreadExecutor("actual-flight-deadline-consumer");
            try {
                var result = executor.submit(() -> {
                    try {
                        var endpoint = new ResultEndpoint("127.0.0.1", server.getPort(), "deadline",
                                ResultProtocol.GRPC.value());
                        if (streaming) {
                            fetcher.fetchStream(endpoint, bytes -> { }, System.currentTimeMillis() + 3_000);
                        } else {
                            fetcher.fetch(endpoint, System.currentTimeMillis() + 3_000);
                        }
                        return (Throwable) null;
                    } catch (Throwable failure) {
                        return failure;
                    }
                });
                assertThat(producer.streamStarted.await(2, TimeUnit.SECONDS)).isTrue();
                Throwable failure = result.get(5, TimeUnit.SECONDS);
                assertThat(failure).isInstanceOf(TransportConnectionException.class);
                assertThat(((TransportConnectionException) failure).transportErrorCode())
                        .isEqualTo("TRANSPORT_QUERY_DEADLINE_EXCEEDED");
            } finally {
                producer.releaseProducer.countDown();
                executor.shutdown();
                boolean producerExited = producer.producerExited.await(5, TimeUnit.SECONDS);
                boolean consumerExited = executor.awaitTermination(5, TimeUnit.SECONDS);
                assertThat(producerExited).isTrue();
                assertThat(consumerExited).isTrue();
            }
            var recovered = ArrowBatchDecoder.decode(fetcher.fetch(new ResultEndpoint("127.0.0.1", server.getPort(),
                    "recovery", ResultProtocol.GRPC.value()), System.currentTimeMillis() + 3_000));
            assertThat(recovered.rows()).singleElement().satisfies(row -> assertThat(row).containsExactly(42));
            fetcher.close();
        }
    }

    private static final class DeadlineProducer extends NoOpFlightProducer {
        private final RootAllocator allocator;
        private final CountDownLatch streamStarted = new CountDownLatch(1);
        private final CountDownLatch releaseProducer = new CountDownLatch(1);
        private final CountDownLatch producerExited = new CountDownLatch(1);

        private DeadlineProducer(RootAllocator allocator) { this.allocator = allocator; }

        @Override
        public void getStream(FlightProducer.CallContext context, Ticket ticket, FlightProducer.ServerStreamListener listener) {
            boolean deadline = new String(ticket.getBytes(), StandardCharsets.UTF_8).equals("deadline");
            try {
                IntVector vector = new IntVector("value", allocator);
                try (VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                    vector.allocateNew();
                    if (!deadline) {
                        vector.setSafe(0, 42);
                        root.setRowCount(1);
                        listener.start(root);
                        listener.putNext();
                        listener.completed();
                        return;
                    }
                    root.setRowCount(0);
                    listener.start(root);
                    streamStarted.countDown();
                    if (releaseProducer.await(10, TimeUnit.SECONDS) && !listener.isCancelled()) listener.completed();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Actual deadline producer was interrupted", interrupted);
            } finally {
                if (deadline) producerExited.countDown();
            }
        }
    }
}
