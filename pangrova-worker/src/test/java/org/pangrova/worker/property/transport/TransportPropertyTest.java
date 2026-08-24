package org.pangrova.worker.property.transport;

import org.pangrova.worker.transport.TransportClient;
import org.pangrova.worker.transport.TransportServer;

import static org.assertj.core.api.Assertions.assertThat;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.pangrova.spi.transport.DataProducer;
import org.pangrova.spi.transport.TransportServerConfig;
import org.pangrova.spi.transport.DataTicket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Property-based tests for gRPC transport.
 * Uses real TransportServer and TransportClient — no mocks.
 *
 * <p>Server is started once per property (port=0 for random binding)
 * and shared across all tries to avoid Netty thread pool exhaustion.</p>
 *
 * <p><b>Validates: Requirements 7.2, 7.4, 8.1, 8.2</b></p>
 */
@org.junit.jupiter.api.Tag("native-required")
class TransportPropertyTest {

    private BufferAllocator testAllocator;
    private final ConcurrentHashMap<String, byte[]> dataRegistry = new ConcurrentHashMap<>();
    private final AtomicInteger ticketCounter = new AtomicInteger(0);
    private TransportServer server;
    private TransportClient client;

    @BeforeProperty
    void setUp() {
        testAllocator = new RootAllocator(Long.MAX_VALUE);
        dataRegistry.clear();
        ticketCounter.set(0);
        // remove() ensures first call returns data, subsequent calls return EOF (empty byte[]).
        // This matches the DataProducer SPI contract: non-empty = data, empty = stream ended.
        DataProducer producer = ticket -> {
            byte[] data = dataRegistry.remove(ticket.queryId());
            return data != null ? data : new byte[0];
        };
        server = new TransportServer(producer, new TransportServerConfig(0));
        server.start();
        client = new TransportClient("localhost", server.getPort());
    }

    @AfterProperty
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
        if (testAllocator != null) {
            testAllocator.close();
        }
    }

    /**
     * Property 10: Transport data round-trip.
     * For any row count, data written as Arrow IPC through DataProducer
     * should be retrievable via TransportClient.doGet with identical schema,
     * row count, and values.
     */
    @Property(tries = 100)
    @Tag("Feature: e2e-pipeline-integration, Property 10: transport data round-trip")
    void transportDataRoundTrip(@ForAll("rowCounts") int rowCount) throws Exception {
        byte[] originalBatchData = buildArrowBatchData(rowCount);
        String ticketId = "rt-" + ticketCounter.incrementAndGet();
        dataRegistry.put(ticketId, originalBatchData);

        DataTicket ticket = new DataTicket(ticketId, "t1", 0, DataTicket.TicketType.RESULT);
        byte[] received = client.doGet(ticket);
        assertRoundTripEquivalence(received, rowCount);
    }

    /**
     * Property 11: TransportServer random port binding.
     * The server started with port=0 in @BeforeProperty must have getPort() > 0
     * and be reachable. Each try verifies the server responds to a unique request.
     */
    @Property(tries = 100)
    @Tag("Feature: e2e-pipeline-integration, Property 11: TransportServer random port binding")
    void transportServerRandomPortBinding(@ForAll("rowCounts") int ignored) {
        assertThat(server.getPort()).isGreaterThan(0);
        assertThat(server.isRunning()).isTrue();

        String probeId = "probe-" + ticketCounter.incrementAndGet();
        DataTicket ticket = new DataTicket(probeId, "t1", 0, DataTicket.TicketType.RESULT);
        byte[] response = client.doGet(ticket);
        assertThat(response).isNotNull();
    }

    @Provide
    Arbitrary<Integer> rowCounts() {
        return Arbitraries.integers().between(0, 200);
    }

    // --- helpers ---

    private byte[] buildArrowBatchData(int rowCount) throws Exception {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));
        try (BufferAllocator child = testAllocator.newChildAllocator("build", 0, Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, child)) {
            populateVectors(root, rowCount);
            return serializeToIpc(root);
        }
    }

    private void populateVectors(VectorSchemaRoot root, int rowCount) {
        IntVector idVec = (IntVector) root.getVector("id");
        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        root.allocateNew();
        for (int i = 0; i < rowCount; i++) {
            idVec.setSafe(i, i);
            nameVec.setSafe(i, ("row-" + i).getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(rowCount);
    }

    private byte[] serializeToIpc(VectorSchemaRoot root) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return baos.toByteArray();
    }

    private void assertRoundTripEquivalence(byte[] received, int expectedRows) throws Exception {
        if (expectedRows == 0) {
            assertEmptyStream(received);
            return;
        }
        // Use child allocator per assertion to isolate buffer lifecycle
        try (BufferAllocator child = testAllocator.newChildAllocator("recv", 0, Long.MAX_VALUE);
             ArrowStreamReader recvReader = new ArrowStreamReader(
                     new ByteArrayInputStream(received), child)) {
            assertThat(recvReader.loadNextBatch()).isTrue();
            VectorSchemaRoot recvRoot = recvReader.getVectorSchemaRoot();
            assertThat(recvRoot.getRowCount()).isEqualTo(expectedRows);
            assertThat(recvRoot.getSchema().getFields()).hasSize(2);
            assertVectorValues(recvRoot, expectedRows);
            while (recvReader.loadNextBatch()) {
                // drain
            }
        }
    }

    private void assertEmptyStream(byte[] received) throws Exception {
        try (BufferAllocator child = testAllocator.newChildAllocator("empty", 0, Long.MAX_VALUE);
             ArrowStreamReader reader = new ArrowStreamReader(
                     new ByteArrayInputStream(received), child)) {
            while (reader.loadNextBatch()) {
                assertThat(reader.getVectorSchemaRoot().getRowCount()).isZero();
            }
        }
    }

    private void assertVectorValues(VectorSchemaRoot root, int rows) {
        IntVector idVec = (IntVector) root.getVector("id");
        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        for (int i = 0; i < rows; i++) {
            assertThat(idVec.get(i)).isEqualTo(i);
            assertThat(new String(nameVec.get(i), StandardCharsets.UTF_8)).isEqualTo("row-" + i);
        }
    }

}
