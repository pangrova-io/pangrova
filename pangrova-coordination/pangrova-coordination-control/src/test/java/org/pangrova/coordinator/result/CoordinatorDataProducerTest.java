package org.pangrova.coordinator.result;

import org.pangrova.protocol.enums.DataTicketType;
import org.pangrova.protocol.exception.DataProducerException;
import org.pangrova.protocol.transport.DataTicket;
import org.pangrova.protocol.enums.TransportStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for CoordinatorDataProducer — request cursors return buffered data
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

        DataTicket ticket = new DataTicket("q1", "t1", 0, DataTicketType.RESULT);
        byte[] result = producer.openStream(ticket).next();

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void doGetThrowsNotFoundForMissingQuery() {
        DataTicket ticket = new DataTicket("missing", "t1", 0, DataTicketType.RESULT);

        assertThatThrownBy(() -> producer.openStream(ticket))
                .isInstanceOf(DataProducerException.class)
                .satisfies(ex -> assertThat(((DataProducerException) ex).getStatus())
                        .isEqualTo(TransportStatus.NOT_FOUND))
                .hasMessageContaining("missing");
    }

    @Test
    void doGetReturnsDataForMultipleQueries() {
        buffer.put("q1", new byte[]{1});
        buffer.put("q2", new byte[]{2});

        assertThat(producer.openStream(ticket("q1")).next()).isEqualTo(new byte[]{1});
        assertThat(producer.openStream(ticket("q2")).next()).isEqualTo(new byte[]{2});
    }

    @Test
    void independentRequestsRetainDataWhileEachCursorReachesEof() {
        buffer.put("q1", new byte[]{42});
        var first = producer.openStream(ticket("q1"));
        var second = producer.openStream(ticket("q1"));
        assertThat(first.next()).containsExactly((byte) 42);
        assertThat(first.next()).isEmpty();
        assertThat(second.next()).containsExactly((byte) 42);
        assertThat(second.next()).isEmpty();
        assertThat(first.next()).isEmpty();
        assertThat(buffer.contains("q1")).isTrue();
    }

    @Test
    void constructorRejectsNullBuffer() {
        assertThatThrownBy(() -> new CoordinatorDataProducer(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("buffer");
    }

    private static DataTicket ticket(String queryId) {
        return new DataTicket(queryId, "t1", 0, DataTicketType.RESULT);
    }
}
