package org.pangrova.coordinator.result;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.junit.jupiter.api.Test;
import org.pangrova.types.value.TypedRow;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatorDataProducerTest {
    @Test
    void cancelledAndFailedTransportsReleaseTheSingleReader() throws Exception {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try (var allocator = new RootAllocator(1024 * 1024);
             var buffer = new CoordinatorResultBuffer(scheduler, 60_000)) {
            var producer = new CoordinatorDataProducer(buffer);
            var writer = new CoordinatorResultWriter(allocator);
            var exits = new AtomicInteger();
            for (boolean cancelled : List.of(true, false)) {
                buffer.publish("q", writer.buildBatch(List.of(TypedRow.builder().putInt("id", 1).build()),
                        List.of("id")), exits::incrementAndGet);
                var listener = new Listener(cancelled);
                producer.getStream(null, new Ticket("q".getBytes(StandardCharsets.UTF_8)), listener);
                assertThat(listener.completed).isFalse();
                if (cancelled) {
                    assertThat(listener.error).isNull();
                } else {
                    assertThat(listener.error).hasMessage("transport failed");
                }
                assertThat(buffer.size()).isZero();
            }
            assertThat(exits.get()).isEqualTo(2);
        } finally {
            scheduler.shutdownNow();
            assertThat(scheduler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void missingResultReportsFlightNotFoundInsteadOfEof() throws Exception {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try (var buffer = new CoordinatorResultBuffer(scheduler, 60_000)) {
            var listener = new Listener(false);
            new CoordinatorDataProducer(buffer).getStream(null, new Ticket(new byte[] {1}), listener);
            assertThat(listener.completed).isFalse();
            assertThat(((FlightRuntimeException) listener.error).status().code())
                    .isEqualTo(CallStatus.NOT_FOUND.code());
        } finally {
            scheduler.shutdownNow();
            assertThat(scheduler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void constructorRejectsNullBuffer() {
        assertThatThrownBy(() -> new CoordinatorDataProducer(null)).isInstanceOf(NullPointerException.class);
    }

    private static final class Listener implements ServerStreamListener {
        private final boolean cancelled;
        private Throwable error;
        private boolean completed;

        private Listener(boolean cancelled) { this.cancelled = cancelled; }
        @Override public boolean isCancelled() { return cancelled; }
        @Override public boolean isReady() { return !cancelled; }
        @Override public void setOnCancelHandler(Runnable handler) { }
        @Override public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) { }
        @Override public void putNext() { throw new IllegalStateException("transport failed"); }
        @Override public void putNext(ArrowBuf metadata) { throw new UnsupportedOperationException(); }
        @Override public void putMetadata(ArrowBuf metadata) { throw new UnsupportedOperationException(); }
        @Override public void error(Throwable failure) { error = failure; }
        @Override public void completed() { completed = true; }
    }
}
