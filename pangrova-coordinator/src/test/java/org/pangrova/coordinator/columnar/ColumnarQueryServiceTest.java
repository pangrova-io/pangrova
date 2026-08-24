package org.pangrova.coordinator.columnar;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test verifying Arrow Flight SQL protocol compatibility.
 * Uses the FlightSqlClient (same client used by ADBC drivers) to validate
 * the full client-server round trip over the columnar protocol.
 *
 * <p>Uses a TestableColumnarQueryService that bypasses QueryManager
 * to isolate the protocol layer from the full query execution pipeline.</p>
 */
class ColumnarQueryServiceTest {

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightSqlClient sqlClient;
    private CoordinatorResultBuffer resultBuffer;
    private ScheduledExecutorService executor;
    private ConcurrentHashMap<String, QueryStateMachine> stateMachines;

    @BeforeEach
    void setUp() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        executor = Executors.newSingleThreadScheduledExecutor();
        resultBuffer = new CoordinatorResultBuffer(executor, 60_000L);
        stateMachines = new ConcurrentHashMap<>();

        ColumnarQueryServiceConfig config = ColumnarQueryServiceConfig.defaults();
        TestableColumnarQueryService service = new TestableColumnarQueryService(
                resultBuffer, allocator, config, stateMachines);

        Location location = Location.forGrpcInsecure("localhost", 0);
        server = FlightServer.builder(allocator, location, service).build();
        server.start();

        Location clientLocation = Location.forGrpcInsecure("localhost", server.getPort());
        FlightClient flightClient = FlightClient.builder(allocator, clientLocation).build();
        sqlClient = new FlightSqlClient(flightClient);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (sqlClient != null) {
            sqlClient.close();
        }
        if (server != null) {
            server.close();
        }
        if (allocator != null) {
            allocator.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void executeStatementReturnsFlightInfoWithTicket() throws Exception {
        FlightInfo info = sqlClient.execute("SELECT 1 AS value");

        assertThat(info).isNotNull();
        assertThat(info.getEndpoints()).hasSize(1);
        String queryId = extractQueryId(info.getEndpoints().get(0).getTicket());
        assertThat(queryId).isNotEmpty();
        assertThat(stateMachines).containsKey(queryId);
    }

    @Test
    void getStreamReturnsResultsWhenAvailable() throws Exception {
        FlightInfo info = sqlClient.execute("SELECT 42 AS answer");
        String queryId = extractQueryId(info.getEndpoints().get(0).getTicket());

        // Simulate query completion by putting result bytes and transitioning state
        byte[] ipcBytes = createSampleBatch();
        resultBuffer.put(queryId, ipcBytes);
        QueryStateMachine sm = stateMachines.get(queryId);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        sm.tryTransition(QueryStateMachine.State.FINISHED);

        try (FlightStream stream = sqlClient.getStream(
                info.getEndpoints().get(0).getTicket())) {
            assertThat(stream.getSchema()).isNotNull();
            assertThat(stream.next()).isTrue();
            VectorSchemaRoot root = stream.getRoot();
            assertThat(root.getRowCount()).isEqualTo(1);
            IntVector vector = (IntVector) root.getVector(0);
            assertThat(vector.get(0)).isEqualTo(42);
        }
    }

    @Test
    void preparedStatementCreateExecuteClose() throws Exception {
        FlightSqlClient.PreparedStatement prepared = sqlClient.prepare("SELECT 1 AS col");
        assertThat(prepared).isNotNull();

        FlightInfo info = prepared.execute();
        assertThat(info).isNotNull();
        assertThat(info.getEndpoints()).hasSize(1);

        prepared.close();
    }

    @Test
    void getStreamReturnsErrorForFailedQuery() throws Exception {
        FlightInfo info = sqlClient.execute("SELECT fail");
        String queryId = extractQueryId(info.getEndpoints().get(0).getTicket());

        // Mark query as failed
        QueryStateMachine sm = stateMachines.get(queryId);
        sm.tryTransition(QueryStateMachine.State.FAILED);

        assertThatThrownBy(() -> {
            try (FlightStream stream = sqlClient.getStream(
                    info.getEndpoints().get(0).getTicket())) {
                stream.next();
            }
        }).hasMessageContaining("Query failed or cancelled");
    }

    @Test
    void dmlViaColumnarProtocolReturnsUnimplemented() {
        assertThatThrownBy(() -> sqlClient.executeUpdate("INSERT INTO t VALUES (1)"))
                .hasMessageContaining("not supported");
    }

    // ── Test Infrastructure ──

    /** Extracts the queryId from a Flight SQL TicketStatementQuery-encoded ticket. */
    private static String extractQueryId(Ticket ticket) {
        try {
            Any any = Any.parseFrom(ticket.getBytes());
            FlightSql.TicketStatementQuery tsq = any.unpack(FlightSql.TicketStatementQuery.class);
            return tsq.getStatementHandle().toStringUtf8();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to decode TicketStatementQuery", e);
        }
    }

    private byte[] createSampleBatch() throws Exception {
        Schema schema = new Schema(List.of(
                Field.nullable("answer", new ArrowType.Int(32, true))));
        try (BufferAllocator childAlloc = allocator.newChildAllocator("test-batch", 0, Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, childAlloc)) {
            root.setRowCount(1);
            IntVector vector = (IntVector) root.getVector(0);
            vector.allocateNew(1);
            vector.set(0, 42);
            vector.setValueCount(1);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                    root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        }
    }
}
