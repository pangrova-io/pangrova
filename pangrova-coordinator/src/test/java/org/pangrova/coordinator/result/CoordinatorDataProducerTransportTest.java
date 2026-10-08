package org.pangrova.coordinator.result;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.transport.DataTicket;
import org.pangrova.spi.transport.TransportServerConfig;
import org.pangrova.worker.transport.TransportClient;
import org.pangrova.worker.transport.TransportServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual TCP/columnar transport with the production bounded statement result producer. */
class CoordinatorDataProducerTransportTest {
    @Test
    void retainedStatementResultEndsAndCanBeReadByIndependentRequests() throws Exception {
        assertTransportRows(List.of(TypedRow.builder().putInt("ack", 42).build()),
                List.of("ack"), List.of(1));
    }

    @Test
    void emptyDdlAcknowledgementEndsWithoutRepeatingZeroRowBatches() throws Exception {
        assertTransportRows(List.of(), List.of(), List.of(0));
    }

    private static void assertTransportRows(List<TypedRow> rows, List<String> columns,
                                            List<Integer> expectedCounts) throws Exception {
        var expiration = Executors.newSingleThreadScheduledExecutor();
        try (var allocator = new RootAllocator(16L * 1024 * 1024);
             var buffer = new CoordinatorResultBuffer(expiration, 60_000L)) {
            byte[] bytes = new CoordinatorResultWriter(allocator).serializeTypedRows(rows, columns);
            buffer.put("statement", bytes);
            var server = new TransportServer(new CoordinatorDataProducer(buffer),
                    new TransportServerConfig(0));
            try {
                server.start();
                for (int request = 0; request < 2; request++) {
                    try (var client = new TransportClient("localhost", server.getPort(), 16 * 1024)) {
                        var counts = new ArrayList<Integer>();
                        var ticket = new DataTicket("statement", "statement", 0,
                                DataTicket.TicketType.RESULT);
                        client.doGetStreamZeroCopy(ticket, batch -> {
                            counts.add(batch.getRowCount());
                            assertThat(counts.size()).isLessThanOrEqualTo(1);
                            if (!columns.isEmpty()) {
                                assertThat(batch.getVector("ack").getObject(0)).isEqualTo(42);
                            }
                        });
                        assertThat(counts).containsExactlyElementsOf(expectedCounts);
                    }
                }
                assertThat(buffer.get("statement")).containsExactly(bytes);
            } finally {
                server.stop();
            }
        } finally {
            expiration.shutdownNow();
            assertThat(expiration.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
