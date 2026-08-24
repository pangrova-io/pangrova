package org.pangrova.coordinator.result;

import org.pangrova.spi.transport.DataProducerException;
import org.pangrova.spi.transport.DataTicket;
import org.pangrova.spi.transport.TransportStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for CoordinatorDataProducer — doGet returns buffered data
 * or throws DataProducerException(NOT_FOUND) for missing queryId.
 * Uses real CoordinatorResultBuffer (no mocks).
 */
class CoordinatorDataProducerTest {

    private ScheduledExecutorService executor;
    private CoordinatorResultBuffer buffer;
    private CoordinatorDataProducer producer;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
        buffer = new CoordinatorResultBuffer(executor, 60_000L);
        producer = new CoordinatorDataProducer(buffer);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void doGetReturnsBufferedData() {
        byte[] expected = {10, 20, 30};
        buffer.put("q1", expected);

        DataTicket ticket = new DataTicket("q1", "t1", 0, DataTicket.TicketType.RESULT);
        byte[] result = producer.doGet(ticket);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void doGetThrowsNotFoundForMissingQuery() {
        DataTicket ticket = new DataTicket("missing", "t1", 0, DataTicket.TicketType.RESULT);

        assertThatThrownBy(() -> producer.doGet(ticket))
                .isInstanceOf(DataProducerException.class)
                .satisfies(ex -> assertThat(((DataProducerException) ex).getStatus())
                        .isEqualTo(TransportStatus.NOT_FOUND))
                .hasMessageContaining("missing");
    }

    @Test
    void doGetReturnsDataForMultipleQueries() {
        buffer.put("q1", new byte[]{1});
        buffer.put("q2", new byte[]{2});

        assertThat(producer.doGet(ticket("q1"))).isEqualTo(new byte[]{1});
        assertThat(producer.doGet(ticket("q2"))).isEqualTo(new byte[]{2});
    }

    @Test
    void doGetRetainsDataAcrossMultipleCalls() {
        buffer.put("q1", new byte[]{42});

        assertThat(producer.doGet(ticket("q1"))).isEqualTo(new byte[]{42});
        assertThat(producer.doGet(ticket("q1"))).isEqualTo(new byte[]{42});
    }

    @Test
    void constructorRejectsNullBuffer() {
        assertThatThrownBy(() -> new CoordinatorDataProducer(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("buffer");
    }

    private static DataTicket ticket(String queryId) {
        return new DataTicket(queryId, "t1", 0, DataTicket.TicketType.RESULT);
    }
}
