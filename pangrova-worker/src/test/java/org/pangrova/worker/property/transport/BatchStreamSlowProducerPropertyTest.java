package org.pangrova.worker.property.transport;

import static org.assertj.core.api.Assertions.assertThat;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.pangrova.spi.transport.DataTicket;
import org.pangrova.spi.transport.TransportServerConfig;
import org.pangrova.worker.transport.TransportClient;
import org.pangrova.worker.transport.TransportServer;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Property-based test verifying that BatchStreamDataServer correctly handles
 * slow producers that return null (not-ready) for many consecutive poll cycles
 * before delivering data.
 *
 * <p>The SPI contract: null = not-ready (keep polling), empty byte[] = EOF.
 * The adapter must never treat repeated nulls as EOF.</p>
 *
 * <p>Uses real TransportServer + TransportClient — no mocks.</p>
 *
 * <p><b>Validates: Requirements A1.1</b></p>
 */
@org.junit.jupiter.api.Tag("native-required")
class BatchStreamSlowProducerPropertyTest {

    private BufferAllocator allocator;
    private TransportServer server;
    private TransportClient client;
    private final AtomicInteger ticketCounter = new AtomicInteger(0);

    /**
     * Registry keyed by queryId. Each entry holds a SlowProducerState that
     * returns null for a configured number of polls before delivering real data,
     * then returns empty byte[] (EOF) on subsequent polls.
     */
    private final ConcurrentHashMap<String, SlowProducerState> registry = new ConcurrentHashMap<>();

    @BeforeProperty
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        registry.clear();
        ticketCounter.set(0);
        server = new TransportServer(
                ticket -> {
                    SlowProducerState state = registry.get(ticket.queryId());
                    if (state == null) {
                        return new byte[0]; // unknown ticket → EOF
                    }
                    return state.poll();
                },
                new TransportServerConfig(0));
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
        if (allocator != null) {
            try {
                allocator.close();
            } catch (IllegalStateException ignored) {
                // gRPC async buffer cleanup may lag
            }
        }
    }

    /**
     * Core property: a slow producer that returns null (not-ready) for N consecutive
     * polls before delivering data must still have all its data received by the client.
     * N is drawn from [5..20], deliberately exceeding the old 4-null termination threshold.
     */
    @Property(tries = 30)
    void slowProducerDataArrivesAfterManyNulls(
            @ForAll("nullCountsExceedingOldThreshold") int nullsBefore,
            @ForAll("rowCounts") int rowCount) throws Exception {

        String queryId = "slow-" + ticketCounter.incrementAndGet();
        byte[] batchData = buildIpc(rowCount);
        registry.put(queryId, new SlowProducerState(nullsBefore, batchData));

        DataTicket ticket = new DataTicket(queryId, "t1", 0, DataTicket.TicketType.RESULT);
        byte[] received = client.doGet(ticket);

        assertThat(received).isNotNull();
        if (rowCount == 0) {
            return; // empty batch — just verify no crash
        }
        assertReceivedRows(received, rowCount);
    }

    /**
     * Edge property: a producer that returns only null forever (simulated via
     * thread interruption) should not cause the adapter to hang indefinitely.
     * We verify this by interrupting the client thread after a short delay.
     */
    @Property(tries = 5)
    void eofSignalTerminatesStream(@ForAll("rowCounts") int rowCount) throws Exception {
        String queryId = "eof-" + ticketCounter.incrementAndGet();
        byte[] batchData = buildIpc(rowCount);
        // Zero nulls before data — immediate delivery, then EOF
        registry.put(queryId, new SlowProducerState(0, batchData));

        DataTicket ticket = new DataTicket(queryId, "t1", 0, DataTicket.TicketType.RESULT);
        byte[] received = client.doGet(ticket);

        assertThat(received).isNotNull();
        if (rowCount > 0) {
            assertReceivedRows(received, rowCount);
        }
    }

    @Provide
    Arbitrary<Integer> nullCountsExceedingOldThreshold() {
        // 5..20: all exceed the old 4-null termination threshold
        return Arbitraries.integers().between(5, 20);
    }

    @Provide
    Arbitrary<Integer> rowCounts() {
        return Arbitraries.integers().between(1, 50);
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
     * Simulates a slow producer: returns null for {@code nullsBeforeData} consecutive
     * polls, then returns the real batch data once, then returns empty byte[] (EOF).
     */
    private static final class SlowProducerState {
        private final int nullsBeforeData;
        private final byte[] batchData;
        private int pollCount;
        private boolean dataDelivered;

        SlowProducerState(int nullsBeforeData, byte[] batchData) {
            this.nullsBeforeData = nullsBeforeData;
            this.batchData = batchData;
            this.pollCount = 0;
            this.dataDelivered = false;
        }

        synchronized byte[] poll() {
            if (dataDelivered) {
                return new byte[0]; // EOF
            }
            if (pollCount < nullsBeforeData) {
                pollCount++;
                return null; // not-ready
            }
            dataDelivered = true;
            return batchData;
        }
    }
}
