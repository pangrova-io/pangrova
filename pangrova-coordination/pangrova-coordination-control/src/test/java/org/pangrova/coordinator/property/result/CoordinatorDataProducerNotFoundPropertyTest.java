package org.pangrova.coordinator.property.result;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.pangrova.coordinator.result.CoordinatorDataProducer;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.protocol.exception.DataProducerException;
import org.pangrova.protocol.transport.DataTicket;
import org.pangrova.protocol.enums.TransportStatus;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Property tests for CoordinatorDataProducer NOT_FOUND semantics.
 *
 * <p>Verifies that missing/expired queryIds always produce a
 * {@link DataProducerException} with {@link TransportStatus#NOT_FOUND},
 * never a false-success empty result (EOF).</p>
 *
 * <p><b>Validates: Requirements A3.3</b></p>
 */
class CoordinatorDataProducerNotFoundPropertyTest {

    private ScheduledExecutorService executor;
    private CoordinatorResultBuffer buffer;
    private CoordinatorDataProducer producer;
    private final AtomicLong idCounter = new AtomicLong();

    @BeforeProperty
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
        buffer = new CoordinatorResultBuffer(executor, 60_000L);
        producer = new CoordinatorDataProducer(buffer);
    }

    @AfterProperty
    void tearDown() {
        buffer.close();
        executor.shutdownNow();
    }

    @Property(tries = 10)
    void missingQueryIdAlwaysThrowsNotFound(
            @ForAll("queryIds") String queryId) {
        DataTicket ticket = resultTicket(queryId);

        DataProducerException ex = catchThrowableOfType(
                () -> producer.openStream(ticket),
                DataProducerException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.getStatus()).isEqualTo(TransportStatus.NOT_FOUND);
        assertThat(ex.getMessage()).contains(queryId);
    }

    @Property(tries = 10)
    void bufferedQueryIdNeverThrows(
            @ForAll("uniqueQueryIds") String queryId,
            @ForAll("payloads") byte[] payload) {
        buffer.put(queryId, payload);
        DataTicket ticket = resultTicket(queryId);

        byte[] result = producer.openStream(ticket).next();

        assertThat(result).isEqualTo(payload);
    }

    @Property(tries = 10)
    void removedQueryIdThrowsNotFound(
            @ForAll("uniqueQueryIds") String queryId,
            @ForAll("payloads") byte[] payload) {
        buffer.put(queryId, payload);
        buffer.remove(queryId);
        DataTicket ticket = resultTicket(queryId);

        DataProducerException ex = catchThrowableOfType(
                () -> producer.openStream(ticket),
                DataProducerException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.getStatus()).isEqualTo(TransportStatus.NOT_FOUND);
    }

    @Provide
    Arbitrary<String> queryIds() {
        return Arbitraries.strings()
                .alpha().numeric()
                .ofMinLength(1).ofMaxLength(32)
                .map(s -> "q-" + s);
    }

    @Provide
    Arbitrary<String> uniqueQueryIds() {
        return Arbitraries.strings()
                .alpha().numeric()
                .ofMinLength(1).ofMaxLength(16)
                .map(s -> "q-" + idCounter.incrementAndGet() + "-" + s);
    }

    @Provide
    Arbitrary<byte[]> payloads() {
        return Arbitraries.bytes().array(byte[].class)
                .ofMinSize(1).ofMaxSize(64);
    }

    private static DataTicket resultTicket(String queryId) {
        return new DataTicket(queryId, "t1", 0, DataTicket.TicketType.RESULT);
    }
}
