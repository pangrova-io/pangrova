package org.pangrova.testing.testkit.result;

import org.pangrova.testing.testkit.enums.ResultScalarType;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.common.concurrency.NotThreadSafe;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.enums.ResultProtocol;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Actual typed result transport; component fixture, not SQL or Native execution. */
@NotThreadSafe
public final class TestResultEndpoint implements AutoCloseable {
    private final RootAllocator allocator = new RootAllocator(32L * 1024 * 1024);
    private final ExecutorService executor = ManagedExecutorFactory.newFixedThreadPool("test-result-endpoint", 2);
    private final String ticket = UUID.randomUUID().toString();
    private FlightServer server;
    private boolean closed;

    /** Starts a real OS-assigned endpoint that emits the supplied value followed by EOF. */
    public TestResultEndpoint(int value) throws Exception {
        this(List.of(new Column("value", ResultScalarType.INTEGER)), List.of(List.of(new IntegerCell(value))));
    }

    /** Starts an actual tabular stream from immutable, framework-free typed test data. */
    public TestResultEndpoint(List<Column> columns, List<List<Cell>> rows) throws Exception {
        try {
            List<Column> names = List.copyOf(columns);
            List<List<Cell>> data = rows.stream().map(List::copyOf).toList();
            for (var row : data) {
                if (row.size() != names.size()) throw new IllegalArgumentException("Row width does not match schema");
                for (int i = 0; i < row.size(); i++) {
                    boolean integer = row.get(i) instanceof IntegerCell;
                    if (integer != (names.get(i).type() == ResultScalarType.INTEGER)) {
                        throw new IllegalArgumentException("Cell type does not match schema");
                    }
                }
            }
            server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0),
                    new TableProducer(names, data)).executor(executor).build().start();
        } catch (Exception | Error failure) {
            try {
                close();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /** Returns only the protocol descriptor, never an SDK object or authorization claim. */
    public ResultEndpoint endpoint() {
        if (closed) throw new IllegalStateException("Result endpoint is closed");
        return new ResultEndpoint("127.0.0.1", server.getPort(), ticket, ResultProtocol.GRPC.value());
    }

    /** Joins real server callbacks before releasing their allocator; failed cleanup remains retryable. */
    @Override public void close() throws Exception {
        if (closed) return;
        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Actual result server has not terminated");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
        }
        ExecutorDrain.shutdownAndAwait(executor, 5_000);
        if (allocator.getAllocatedMemory() != 0 || !allocator.getChildAllocators().isEmpty()) {
            throw new IllegalStateException("Actual result producer still owns allocator resources");
        }
        allocator.close();
        closed = true;
    }

    /** Immutable test column description with no SDK dependency in its signature. */
    public record Column(String name, ResultScalarType type) {
        public Column {
            java.util.Objects.requireNonNull(name);
            java.util.Objects.requireNonNull(type);
        }
    }

    /** Typed immutable test cell. */
    public sealed interface Cell permits IntegerCell, TextCell {}

    /** An integer cell. */
    public record IntegerCell(int value) implements Cell {}

    /** A text cell. */
    public record TextCell(String value) implements Cell {
        public TextCell { java.util.Objects.requireNonNull(value); }
    }

    private final class TableProducer extends NoOpFlightProducer {
        private final Schema schema;
        private final List<List<Cell>> rows;

        private TableProducer(List<Column> columns, List<List<Cell>> rows) {
            schema = new Schema(columns.stream().map(column -> new Field(column.name(), FieldType.nullable(
                    column.type() == ResultScalarType.INTEGER ? new ArrowType.Int(32, true) : new ArrowType.Utf8()),
                    null)).toList());
            this.rows = rows;
        }

        @Override public void getStream(FlightProducer.CallContext context, Ticket request,
                                       FlightProducer.ServerStreamListener listener) {
            if (!ticket.equals(new String(request.getBytes(), StandardCharsets.UTF_8))) {
                listener.error(CallStatus.NOT_FOUND.withDescription("Unknown result ticket").toRuntimeException());
                return;
            }
            try (var root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();
                for (int row = 0; row < rows.size(); row++) {
                    for (int column = 0; column < rows.get(row).size(); column++) {
                        Cell cell = rows.get(row).get(column);
                        if (cell instanceof IntegerCell integer) {
                            ((IntVector) root.getVector(column)).setSafe(row, integer.value());
                        } else {
                            ((VarCharVector) root.getVector(column)).setSafe(row,
                                    ((TextCell) cell).value().getBytes(StandardCharsets.UTF_8));
                        }
                    }
                }
                root.setRowCount(rows.size());
                listener.start(root);
                listener.putNext();
                listener.completed();
            }
        }
    }
}
