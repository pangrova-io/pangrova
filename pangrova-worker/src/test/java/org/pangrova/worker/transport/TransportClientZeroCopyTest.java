package org.pangrova.worker.transport;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.spi.transport.DataProducer;
import org.pangrova.spi.transport.TransportServerConfig;
import org.pangrova.spi.transport.DataTicket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tests for {@link TransportClient#doGetStreamZeroCopy} — verifies that
 * VectorSchemaRoot batches are delivered directly without byte[] serialization.
 * Uses real TransportServer and TransportClient — no mocks.
 */
@Tag("native-required")
class TransportClientZeroCopyTest {

    private BufferAllocator testAllocator;
    private final ConcurrentHashMap<String, byte[]> dataRegistry = new ConcurrentHashMap<>();
    private TransportServer server;
    private TransportClient client;

    @BeforeEach
    void setUp() {
        testAllocator = new RootAllocator(64 * 1024 * 1024);
        dataRegistry.clear();
        // remove() ensures first call returns data, subsequent calls return EOF (empty byte[]).
        // This matches the DataProducer SPI contract: non-empty = data, empty = stream ended.
        DataProducer producer = ticket -> () -> {
            byte[] data = dataRegistry.remove(ticket.queryId());
            return data != null ? data : new byte[0];
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
        if (testAllocator != null) {
            testAllocator.close();
        }
    }

    @Test
    void zeroCopyStreamDeliversCorrectRowCount() throws Exception {
        int rowCount = 50;
        byte[] ipc = buildBatchData(rowCount);
        dataRegistry.put("zc-1", ipc);

        DataTicket ticket = new DataTicket("zc-1", "t1", 0, DataTicket.TicketType.SHUFFLE);
        List<Integer> rowCounts = new ArrayList<>();

        client.doGetStreamZeroCopy(ticket, batch -> rowCounts.add(batch.getRowCount()));

        assertThat(rowCounts).hasSize(1);
        assertThat(rowCounts.get(0)).isEqualTo(rowCount);
    }

    @Test
    void zeroCopyStreamDeliversCorrectValues() throws Exception {
        int rowCount = 10;
        byte[] ipc = buildBatchData(rowCount);
        dataRegistry.put("zc-2", ipc);

        DataTicket ticket = new DataTicket("zc-2", "t1", 0, DataTicket.TicketType.SHUFFLE);
        List<String> names = new ArrayList<>();

        client.doGetStreamZeroCopy(ticket, batch -> {
            VarCharVector nameVec = (VarCharVector) batch.getVector("name");
            for (int i = 0; i < batch.getRowCount(); i++) {
                names.add(new String(nameVec.get(i), StandardCharsets.UTF_8));
            }
        });

        assertThat(names).hasSize(rowCount);
        for (int i = 0; i < rowCount; i++) {
            assertThat(names.get(i)).isEqualTo("row-" + i);
        }
    }

    @Test
    void zeroCopyStreamTracksUncompressedBytes() throws Exception {
        byte[] ipc = buildBatchData(20);
        dataRegistry.put("zc-3", ipc);

        client.resetBytesTransferred();
        DataTicket ticket = new DataTicket("zc-3", "t1", 0, DataTicket.TicketType.SHUFFLE);

        client.doGetStreamZeroCopy(ticket, batch -> { /* consume */ });

        assertThat(client.getUncompressedBytesTransferred()).isGreaterThan(0);
    }

    @Test
    void zeroCopyStreamHandlesEmptyData() {
        dataRegistry.put("zc-empty", new byte[0]);
        DataTicket ticket = new DataTicket("zc-empty", "t1", 0, DataTicket.TicketType.SHUFFLE);
        List<Integer> rowCounts = new ArrayList<>();

        client.doGetStreamZeroCopy(ticket, batch -> rowCounts.add(batch.getRowCount()));

        assertThat(rowCounts).isEmpty();
    }

    @Test
    void zeroCopyStreamWithExternalAllocator() throws Exception {
        BufferAllocator external = testAllocator.newChildAllocator("ext-client", 0, 32 * 1024 * 1024);
        TransportClient extClient = new TransportClient("localhost", server.getPort(), external, 256 * 1024 * 1024);
        try {
            byte[] ipc = buildBatchData(5);
            dataRegistry.put("zc-ext", ipc);
            DataTicket ticket = new DataTicket("zc-ext", "t1", 0, DataTicket.TicketType.SHUFFLE);
            List<Integer> rowCounts = new ArrayList<>();

            extClient.doGetStreamZeroCopy(ticket, batch -> rowCounts.add(batch.getRowCount()));

            assertThat(rowCounts).containsExactly(5);
        } finally {
            extClient.close();
            external.close();
        }
    }

    private byte[] buildBatchData(int rowCount) throws Exception {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        try (BufferAllocator child = testAllocator.newChildAllocator("build", 0, Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, child)) {
            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            root.allocateNew();
            for (int i = 0; i < rowCount; i++) {
                idVec.setSafe(i, i);
                nameVec.setSafe(i, ("row-" + i).getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(rowCount);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }
}
