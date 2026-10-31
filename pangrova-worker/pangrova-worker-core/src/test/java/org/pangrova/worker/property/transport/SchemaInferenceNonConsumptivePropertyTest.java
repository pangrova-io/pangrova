package org.pangrova.worker.property.transport;

import static org.assertj.core.api.Assertions.assertThat;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.pangrova.protocol.transport.DataTicket;
import org.pangrova.protocol.transport.TransportServerConfig;
import org.pangrova.worker.transport.TransportClient;
import org.pangrova.worker.transport.TransportServer;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Property test verifying that getFlightInfo() is non-consumptive:
 * calling getFlightInfo() before doGet() must not lose any batches.
 *
 * <p><b>Validates: Requirements A1.2</b></p>
 */
@org.junit.jupiter.api.Tag("native-required")
class SchemaInferenceNonConsumptivePropertyTest {

    private BufferAllocator allocator;
    private TransportServer server;
    private TransportClient client;
    private FlightClient flightClient;
    private final AtomicInteger ticketCounter = new AtomicInteger(0);
    private final ConcurrentHashMap<String, OneShotBatch> registry = new ConcurrentHashMap<>();

    @BeforeProperty
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        registry.clear();
        ticketCounter.set(0);
        server = new TransportServer(
                ticket -> () -> {
                    OneShotBatch state = registry.get(ticket.queryId());
                    if (state == null) {
                        return new byte[0]; // unknown ticket → EOF
                    }
                    return state.poll();
                },
                new TransportServerConfig(0));
        server.start();
        client = new TransportClient("localhost", server.getPort());
        flightClient = FlightClient.builder(allocator,
                Location.forGrpcInsecure("localhost", server.getPort())).build();
    }

    @AfterProperty
    void tearDown() {
        if (flightClient != null) {
            try { flightClient.close(); } catch (InterruptedException ignored) { }
        }
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
        if (allocator != null) {
            try { allocator.close(); } catch (IllegalStateException ignored) { }
        }
    }

    /**
     * Core property: for any row count, calling getFlightInfo() before doGet()
     * must not consume data. The subsequent doGet() must return all rows.
     */
    @Property(tries = 30)
    void getFlightInfoThenDoGetReturnsAllRows(@ForAll("rowCounts") int rowCount) throws Exception {
        String queryId = "schema-" + ticketCounter.incrementAndGet();
        byte[] batchData = buildIpc(rowCount);
        registry.put(queryId, new OneShotBatch(batchData));

        DataTicket ticket = new DataTicket(queryId, "t1", 0, DataTicket.TicketType.RESULT);

        // Step 1: call getFlightInfo — must NOT consume data
        FlightDescriptor descriptor = FlightDescriptor.command(ticket.serialize());
        FlightInfo info = flightClient.getInfo(descriptor);
        assertThat(info).isNotNull();
        assertThat(info.getEndpoints()).isNotEmpty();

        // Step 2: call doGet — must return ALL rows including the first batch
        byte[] received = client.doGet(ticket);
        assertThat(received).isNotNull();
        assertReceivedRows(received, rowCount);
    }

    /**
     * Complementary property: doGet() without prior getFlightInfo() must also
     * return all rows (baseline correctness).
     */
    @Property(tries = 20)
    void doGetWithoutGetFlightInfoReturnsAllRows(@ForAll("rowCounts") int rowCount) throws Exception {
        String queryId = "direct-" + ticketCounter.incrementAndGet();
        byte[] batchData = buildIpc(rowCount);
        registry.put(queryId, new OneShotBatch(batchData));

        DataTicket ticket = new DataTicket(queryId, "t1", 0, DataTicket.TicketType.RESULT);
        byte[] received = client.doGet(ticket);

        assertThat(received).isNotNull();
        assertReceivedRows(received, rowCount);
    }

    @Provide
    Arbitrary<Integer> rowCounts() {
        return Arbitraries.integers().between(1, 100);
    }

    // --- helpers ---

    private byte[] buildIpc(int rows) throws Exception {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        try (BufferAllocator child = allocator.newChildAllocator("build", 0, Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, child)) {
            root.allocateNew();
            IntVector vec = (IntVector) root.getVector("id");
            for (int i = 0; i < rows; i++) {
                vec.setSafe(i, i);
            }
            root.setRowCount(rows);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        }
    }

    private void assertReceivedRows(byte[] received, int expectedRows) throws Exception {
        try (BufferAllocator child = allocator.newChildAllocator("recv", 0, Long.MAX_VALUE);
             ArrowStreamReader reader = new ArrowStreamReader(
                     new ByteArrayInputStream(received), child)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(expectedRows);
            IntVector idVec = (IntVector) root.getVector("id");
            for (int i = 0; i < expectedRows; i++) {
                assertThat(idVec.get(i)).isEqualTo(i);
            }
        }
    }

    /**
     * Delivers batch data exactly once, then returns empty bytes (EOF).
     * Thread-safe for concurrent access from transport threads.
     */
    private static final class OneShotBatch {
        private final byte[] batchData;
        private final AtomicBoolean delivered = new AtomicBoolean(false);

        OneShotBatch(byte[] batchData) {
            this.batchData = batchData;
        }

        byte[] poll() {
            if (delivered.compareAndSet(false, true)) {
                return batchData;
            }
            return new byte[0]; // EOF
        }
    }
}
