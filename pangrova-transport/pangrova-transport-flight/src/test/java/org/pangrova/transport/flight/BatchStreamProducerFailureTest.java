package org.pangrova.transport.flight;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.protocol.transport.DataProducer;
import org.pangrova.protocol.transport.DataProducerException;
import org.pangrova.protocol.transport.DataTicket;
import org.pangrova.protocol.transport.TransportStatus;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual TCP stream failures must never become a successful partial result. */
class BatchStreamProducerFailureTest {
    private final RootAllocator allocator = new RootAllocator(16 * 1024 * 1024);
    private final ExecutorService filler = ManagedExecutorFactory.newSingleThreadExecutor("stream-failure-test");

    @AfterEach
    void closeOwners() throws Exception {
        filler.shutdownNow();
        assertThat(filler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        allocator.close();
    }

    @Test
    void producerFailureAfterFirstBatchPreservesFailureAtClient() throws Exception {
        byte[] batch = batch();
        DataProducer producer = ticket -> {
            AtomicInteger reads = new AtomicInteger();
            return () -> {
                if (reads.getAndIncrement() == 0) return batch;
                throw new IllegalStateException("Exact producer cursor failed");
            };
        };
        withStream(producer, stream -> {
            assertThat(stream.next()).isTrue();
            assertThat(stream.getRoot().getRowCount()).isEqualTo(1);
            assertThatThrownBy(stream::next).hasMessageContaining("Exact producer cursor failed");
        });
    }

    @ParameterizedTest
    @EnumSource(value = TransportStatus.class, names = "OK", mode = EnumSource.Mode.EXCLUDE)
    void typedOpenFailureRetainsItsWireStatus(TransportStatus status) throws Exception {
        withStream(ticket -> {
            throw new DataProducerException(status, "Typed open failure");
        }, stream -> assertWireFailure(stream, status, "Typed open failure"));
    }

    @ParameterizedTest
    @EnumSource(value = TransportStatus.class, names = "OK", mode = EnumSource.Mode.EXCLUDE)
    void typedAsyncCursorFailureRetainsItsWireStatus(TransportStatus status) throws Exception {
        byte[] batch = batch();
        DataProducer producer = ticket -> {
            AtomicInteger reads = new AtomicInteger();
            return () -> {
                if (reads.getAndIncrement() == 0) return batch;
                throw new DataProducerException(status, "Typed cursor failure");
            };
        };
        withStream(producer, stream -> {
            assertThat(stream.next()).isTrue();
            assertWireFailure(stream, status, "Typed cursor failure");
        });
    }

    @Test
    void exceptionWithOkStatusCannotBecomeSuccessfulEndOfStream() throws Exception {
        withStream(ticket -> {
            throw new DataProducerException(TransportStatus.OK, "Invalid success exception");
        }, stream -> assertWireFailure(stream, TransportStatus.INTERNAL, "Invalid success exception"));
    }

    private static void assertWireFailure(FlightStream stream, TransportStatus status, String message) {
        assertThatThrownBy(stream::next).isInstanceOfSatisfying(FlightRuntimeException.class, failure -> {
            assertThat(failure.status().code()).isEqualTo(FlightStatusCode.valueOf(status.name()));
            assertThat(failure.status().description()).contains(message);
        });
    }

    @Test
    void nextBatchTimeoutIsFailureNotEndOfStream() throws Exception {
        byte[] batch = batch();
        DataProducer producer = ticket -> {
            AtomicInteger reads = new AtomicInteger();
            return () -> reads.getAndIncrement() == 0 ? batch : null;
        };
        withStream(producer, stream -> {
            assertThat(stream.next()).isTrue();
            assertThatThrownBy(stream::next).hasMessageContaining("Timed out");
        });
    }

    @Test
    void firstBatchTimeoutIsFailureNotEmptyResponse() throws Exception {
        withStream(ticket -> () -> null, stream ->
                assertThatThrownBy(stream::next).hasMessageContaining("Timed out"));
    }

    @Test
    void explicitEndAfterFirstBatchCompletesNormally() throws Exception {
        byte[] batch = batch();
        DataProducer producer = ticket -> {
            AtomicInteger reads = new AtomicInteger();
            return () -> reads.getAndIncrement() == 0 ? batch : new byte[0];
        };
        withStream(producer, stream -> {
            assertThat(stream.next()).isTrue();
            assertThat(stream.next()).isFalse();
        });
    }

    @Test
    void rejectedFillAdmissionIsFailureNotSuccessfulFirstBatch() throws Exception {
        byte[] batch = batch();
        filler.shutdown();
        withStream(ticket -> () -> batch, stream -> {
            assertThat(stream.next()).isTrue();
            assertThatThrownBy(stream::next).hasMessageContaining("rejected");
        });
    }

    private void withStream(DataProducer producer, StreamAssertion assertion) throws Exception {
        BatchStreamProducer adapter = new BatchStreamProducer(producer, allocator, null, filler, 300, 300);
        try (FlightServer server = FlightServer.builder(allocator,
                Location.forGrpcInsecure("127.0.0.1", 0), adapter).build()) {
            server.start();
            try (FlightClient client = FlightClient.builder(allocator,
                    Location.forGrpcInsecure("127.0.0.1", server.getPort())).build();
                 FlightStream stream = client.getStream(new Ticket(new DataTicket(
                         "cursor-failure", "result", 0, DataTicket.TicketType.RESULT).serialize()))) {
                assertion.verify(stream);
            }
        }
    }

    private byte[] batch() throws Exception {
        Schema schema = new Schema(List.of(new Field("id",
                FieldType.nullable(new ArrowType.Int(32, true)), null)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            ((IntVector) root.getVector("id")).setSafe(0, 42);
            root.setRowCount(1);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return bytes.toByteArray();
        }
    }

    @FunctionalInterface
    private interface StreamAssertion {
        void verify(FlightStream stream) throws Exception;
    }
}
