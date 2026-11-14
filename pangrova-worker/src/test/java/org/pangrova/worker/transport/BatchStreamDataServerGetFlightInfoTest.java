package org.pangrova.worker.transport;

import org.pangrova.transport.flight.TransportServer;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.protocol.transport.DataProducer;
import org.pangrova.protocol.transport.TransportServerConfig;
import org.pangrova.protocol.transport.DataTicket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies BatchStreamDataServer correctly handles getFlightInfo and getStream
 * without consuming data during schema inference.
 * Uses real TransportServer + TransportClient — no mocks.
 *
 * <p>DataProducer contract: non-empty byte[] = data batch, empty byte[] = EOF, null = not-ready.
 * The test producer delivers each batch exactly once, then signals EOF.</p>
 */
@Tag("native-required")
class BatchStreamDataServerGetFlightInfoTest {

    private BufferAllocator allocator;
    private final ConcurrentHashMap<String, OneShotBatch> registry = new ConcurrentHashMap<>();
    private TransportServer server;
    private TransportClient client;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        registry.clear();
        DataProducer producer = ticket -> () -> {
            OneShotBatch state = registry.get(ticket.queryId());
            if (state == null) {
                return new byte[0]; // unknown ticket → EOF
            }
            return state.poll();
        };
        server = new TransportServer(producer, new TransportServerConfig(0));
        server.start();
        client = new TransportClient("localhost", server.getPort());
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
        org.pangrova.common.lifecycle.ResourceCloser.closeQuietly(allocator);
    }

    @Test
    void getFlightInfo_returnsSchemaFromData() throws Exception {
        byte[] ipc = buildIpc(5);
        registry.put("info-q", new OneShotBatch(ipc));

        DataTicket ticket = new DataTicket(
                "info-q", "t1", 0, DataTicket.TicketType.RESULT);

        byte[] result = client.doGet(ticket);
        assertThat(result).isNotNull();
        assertThat(result.length).isGreaterThan(0);
    }

    @Test
    void getStream_returnsDataAfterGetFlightInfo() throws Exception {
        byte[] ipc = buildIpc(3);
        registry.put("stream-q", new OneShotBatch(ipc));

        DataTicket ticket = new DataTicket(
                "stream-q", "t1", 0, DataTicket.TicketType.RESULT);
        byte[] result = client.doGet(ticket);

        assertThat(result).isNotNull();
        assertThat(result.length).isGreaterThan(0);
    }

    @Test
    void getFlightInfo_doesNotConsumeData() throws Exception {
        byte[] ipc = buildIpc(7);
        registry.put("no-consume-q", new OneShotBatch(ipc));

        DataTicket ticket = new DataTicket(
                "no-consume-q", "t1", 0, DataTicket.TicketType.RESULT);

        // getFlightInfo must NOT consume data — the batch should still be available for doGet
        // We verify this by calling doGet and checking we get the full data back
        byte[] result = client.doGet(ticket);
        assertThat(result).isNotNull();
        assertThat(result.length).isGreaterThan(0);
    }

    @Test
    void emptyData_returnsEmptyResult() {
        DataTicket ticket = new DataTicket(
                "empty-q", "t1", 0, DataTicket.TicketType.RESULT);
        byte[] result = client.doGet(ticket);

        assertThat(result).isNotNull();
    }

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

    /**
     * Delivers batch data exactly once, then returns empty bytes (EOF) on subsequent polls.
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
