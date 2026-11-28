package org.pangrova.coordinator.property.result;


import org.apache.arrow.memory.RootAllocator;
import org.pangrova.coordinator.result.CoordinatorResultWriter;
import org.pangrova.types.value.TypedRow;

import java.util.List;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.result.CoordinatorResultTestAccess.streamedRoot;

/** Exercises lookup, payload and retirement across generated result identities. */
class CoordinatorDataProducerNotFoundPropertyTest {

    private ScheduledExecutorService executor;
    private CoordinatorResultBuffer buffer;
    private RootAllocator allocator;
    private CoordinatorResultWriter writer;
    private final AtomicLong idCounter = new AtomicLong();

    @BeforeProperty
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
        buffer = new CoordinatorResultBuffer(executor, 60_000L);
        allocator = new RootAllocator(1024 * 1024);
        writer = new CoordinatorResultWriter(allocator);
    }

    @AfterProperty
    void tearDown() {
        buffer.close();
        executor.shutdownNow();
        allocator.close();
    }

    @Property(tries = 10)
    void missingQueryIdCannotAcquireAResult(@ForAll("queryIds") String queryId) {
        assertThat(buffer.claim(queryId)).isNull();
    }

    @Property(tries = 10)
    void bufferedQueryIdExposesItsExactPayloadOnce(
            @ForAll("uniqueQueryIds") String queryId, @ForAll("payloads") byte[] payload) {
        publish(queryId, payload);
        try (var lease = buffer.claim(queryId)) {
            assertThat((byte[]) streamedRoot(lease::streamTo).getVector("value").getObject(0)).isEqualTo(payload);
            assertThat(buffer.claim(queryId)).isNull();
        }
        assertThat(buffer.claim(queryId)).isNull();
    }

    @Property(tries = 10)
    void removedQueryIdCannotAcquireAResult(
            @ForAll("uniqueQueryIds") String queryId, @ForAll("payloads") byte[] payload) {
        publish(queryId, payload);
        buffer.discardUnopened(queryId);
        assertThat(buffer.claim(queryId)).isNull();
    }

    private void publish(String queryId, byte[] payload) {
        var row = TypedRow.builder().putRaw("value", payload).build();
        buffer.publish(queryId, writer.buildBatch(List.of(row), List.of("value")), () -> { });
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
}
