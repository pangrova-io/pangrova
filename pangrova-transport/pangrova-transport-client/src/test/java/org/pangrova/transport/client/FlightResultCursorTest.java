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
package org.pangrova.transport.client;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.transport.client.security.ResultTransportSecurity;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class FlightResultCursorTest {
    private static final int BATCHES = 96;
    private static final int ROWS = 256;
    private static final int CELL_BYTES = 512;
    private static final long WINDOW_BYTES = 8L * 1024 * 1024;

    @Test
    void readsOrderedResultLargerThanAllocatorWindowAndDetachesValues() throws Exception {
        try (var allocator = new RootAllocator(32L * 1024 * 1024);
             var producer = new BatchProducer(allocator, false)) {
            try (var server = server(allocator, producer);
                 var fetcher = new FlightResultFetcher(ResultTransportSecurity.systemTrust(false), WINDOW_BYTES);
                 var cursor = fetcher.open(endpoint(server), deadline())) {
                assertThat(cursor.columns()).extracting(ColumnDescriptor::name).containsExactly("id", "payload");
                assertThat(cursor.next()).isTrue();
                assertThat(cursor.value(0)).isEqualTo(0);
                byte[] detached = (byte[]) cursor.value(1);
                assertThat(detached).hasSize(CELL_BYTES);
                for (int row = 1; row < BATCHES * ROWS; row++) {
                    assertThat(cursor.next()).isTrue();
                    assertThat(cursor.value(0)).isEqualTo(row);
                    if (row % ROWS == 0) {
                        assertThat(((byte[]) cursor.value(1))[0]).isEqualTo((byte) (row / ROWS));
                    }
                }
                assertThat(cursor.next()).isFalse();
                assertThat(detached[0]).isZero();
                assertThat((long) BATCHES * ROWS * CELL_BYTES).isGreaterThan(WINDOW_BYTES);
                assertThat(fetcher.peakAllocatedBytes()).isPositive().isLessThan(WINDOW_BYTES);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void closingBeforeEofCancelsTheActualFlightCallAndReleasesTheClient() throws Exception {
        try (var allocator = new RootAllocator(32L * 1024 * 1024);
             var producer = new BatchProducer(allocator, true)) {
            try (var server = server(allocator, producer); var fetcher = new FlightResultFetcher()) {
                try (var cursor = fetcher.open(endpoint(server), deadline())) {
                    assertThat(cursor.next()).isTrue();
                    assertThat(cursor.value(0)).isEqualTo(0);
                }
                assertThat(producer.cancelled.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(producer.exited.await(5, TimeUnit.SECONDS)).isTrue();
                fetcher.close();
            }
        }
    }

    private static long deadline() {
        return System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(15);
    }

    private static FlightServer server(RootAllocator allocator, FlightProducer producer) throws Exception {
        return FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), producer).build().start();
    }

    private static ResultEndpoint endpoint(FlightServer server) {
        return new ResultEndpoint("127.0.0.1", server.getPort(), "test", ResultProtocol.GRPC.value());
    }

    private static final class BatchProducer extends NoOpFlightProducer implements AutoCloseable {
        private final ExecutorService writer = ManagedExecutorFactory.newSingleThreadExecutor("flight-cursor-test-producer");
        private final RootAllocator allocator;
        private final boolean leaveOpen;
        private final CountDownLatch cancelled = new CountDownLatch(1);
        private final CountDownLatch exited = new CountDownLatch(1);

        BatchProducer(RootAllocator allocator, boolean leaveOpen) {
            this.allocator = allocator;
            this.leaveOpen = leaveOpen;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            var ready = new Semaphore(0);
            listener.setOnReadyHandler(ready::release);
            listener.setOnCancelHandler(() -> {
                cancelled.countDown();
                ready.release();
            });
            writer.execute(() -> publish(listener, ready));
        }

        private void publish(ServerStreamListener listener, Semaphore ready) {
            var ids = new IntVector("id", allocator);
            var binary = new VarBinaryVector("payload", allocator);
            try (var root = VectorSchemaRoot.of(ids, binary)) {
                root.allocateNew();
                listener.start(root);
                for (int batch = 0; batch < (leaveOpen ? 1 : BATCHES); batch++) {
                    while (!listener.isReady() && !listener.isCancelled()) {
                        if (!ready.tryAcquire(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Flight producer did not receive flow credit");
                        }
                    }
                    if (listener.isCancelled()) return;
                    var bytes = new byte[CELL_BYTES];
                    bytes[0] = (byte) batch;
                    for (int row = 0; row < ROWS; row++) {
                        ids.setSafe(row, batch * ROWS + row);
                        binary.setSafe(row, bytes);
                    }
                    root.setRowCount(ROWS);
                    listener.putNext();
                }
                if (!leaveOpen) listener.completed();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                listener.error(interrupted);
            } catch (RuntimeException failure) {
                listener.error(failure);
            } finally {
                exited.countDown();
            }
        }

        @Override
        public void close() throws InterruptedException {
            writer.shutdown();
            assertThat(writer.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
