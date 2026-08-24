package org.pangrova.worker.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.spi.transport.DataProducer;
import org.pangrova.spi.transport.TransportServerConfig;
import org.pangrova.spi.transport.DataTicket;
import org.pangrova.spi.transport.ResultTooLargeException;
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

/**
 * Verifies TransportClient enforces maxResultBytes limit.
 * Uses real TransportServer — no mocks.
 */
@Tag("native-required")
class TransportClientResultSizeLimitTest {

    private BufferAllocator allocator;
    private final ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();
    private TransportServer server;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        // First call returns data, subsequent calls return empty (EOF)
        ConcurrentHashMap<String, Boolean> consumed = new ConcurrentHashMap<>();
        DataProducer producer = ticket -> {
            String qid = ticket.queryId();
            if (consumed.putIfAbsent(qid, Boolean.TRUE) != null) {
                return new byte[0]; // EOF after first delivery
            }
            return data.getOrDefault(qid, new byte[0]);
        };
        server = new TransportServer(producer, new TransportServerConfig(0));
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
        org.pangrova.common.lifecycle.ResourceCloser.closeQuietly(allocator);
    }

    @Test
    void doGet_throwsWhenResultExceedsLimit() throws Exception {
        byte[] largeIpc = buildLargeIpc(10_000);
        data.put("large-q", largeIpc);

        // Set a very small limit (1KB) so the large result triggers it
        try (TransportClient client = new TransportClient("localhost", server.getPort(), 1024)) {
            DataTicket ticket = new DataTicket("large-q", "large-q", 0, DataTicket.TicketType.RESULT);

            assertThatThrownBy(() -> client.doGet(ticket))
                    .isInstanceOf(IllegalStateException.class)
                    .hasCauseInstanceOf(ResultTooLargeException.class);
        }
    }

    @Test
    void doGet_succeedsWithinDefaultLimit() throws Exception {
        byte[] smallIpc = buildLargeIpc(10);
        data.put("small-q", smallIpc);

        try (TransportClient client = new TransportClient("localhost", server.getPort())) {
            DataTicket ticket = new DataTicket("small-q", "small-q", 0, DataTicket.TicketType.RESULT);
            byte[] result = client.doGet(ticket);
            assertThat(result).isNotNull();
            assertThat(result.length).isGreaterThan(0);
        }
    }

    @Test
    void resultTooLargeException_containsLimitInfo() {
        ResultTooLargeException ex = new ResultTooLargeException(100, 200);
        assertThat(ex.getMaxBytes()).isEqualTo(100);
        assertThat(ex.getActualBytes()).isEqualTo(200);
        assertThat(ex.getMessage()).contains("200").contains("100");
    }

    private byte[] buildLargeIpc(int rows) throws Exception {
        Schema schema = new Schema(List.of(
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        try (BufferAllocator child = allocator.newChildAllocator("build", 0, Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, child)) {
            root.allocateNew();
            IntVector vec = (IntVector) root.getVector("val");
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

}
