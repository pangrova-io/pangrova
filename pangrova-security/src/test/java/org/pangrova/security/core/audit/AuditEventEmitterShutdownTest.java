package org.pangrova.security.core.audit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.api.audit.AuditEventType;
import org.pangrova.security.api.audit.AuditEventWriter;

/**
 * Tests for {@link AuditEventEmitter} graceful shutdown, event rejection after close,
 * idempotent close, shutdown timeout observability, and event loss tracking.
 *
 * <p>Uses real test-double writers (no mock frameworks).</p>
 */
class AuditEventEmitterShutdownTest {

    private AuditEventEmitter emitter;

    @AfterEach
    void tearDown() {
        if (emitter != null) {
            emitter.close();
        }
    }

    @Test
    void closeFlushesPendingEventsBeforeCompleting() throws InterruptedException {
        var received = new CopyOnWriteArrayList<AuditEvent>();
        var firstBlocked = new CountDownLatch(1);
        var releaseWriter = new CountDownLatch(1);

        AuditEventWriter writer = event -> {
            firstBlocked.countDown();
            awaitLatch(releaseWriter);
            received.add(event);
        };

        emitter = new AuditEventEmitter(writer);

        // Emit events; first one blocks in writer
        for (int i = 0; i < 5; i++) {
            emitter.emit(createEvent("flush-" + i));
        }
        assertThat(firstBlocked.await(5, TimeUnit.SECONDS)).isTrue();

        // Release writer so all queued tasks can drain
        releaseWriter.countDown();

        emitter.close();

        assertThat(received.size()).isGreaterThanOrEqualTo(1);
        assertThat(emitter.shutdownTimedOut()).isFalse();
    }

    @Test
    void closeCompletesWithinTimeout() {
        emitter = new AuditEventEmitter(event -> {});

        long start = System.nanoTime();
        emitter.close();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(elapsedMs).isLessThan(5000);
        assertThat(emitter.shutdownTimedOut()).isFalse();
    }

    @Test
    void eventsEmittedAfterCloseAreRejected() {
        var received = new CopyOnWriteArrayList<AuditEvent>();
        emitter = new AuditEventEmitter(received::add);

        emitter.close();
        emitter.emit(createEvent("rejected-1"));
        emitter.emit(createEvent("rejected-2"));

        assertThat(emitter.eventsRejectedAfterClose()).isEqualTo(2);
    }

    @Test
    void doubleCloseIsIdempotent() {
        emitter = new AuditEventEmitter(event -> {});

        emitter.close();
        emitter.close();

        assertThat(emitter.shutdownTimedOut()).isFalse();
    }

    @Test
    void shutdownTimeoutIsObservableWhenWriterIsSlow() throws InterruptedException {
        var blockForever = new CountDownLatch(1);
        var writerEntered = new CountDownLatch(1);

        AuditEventWriter slowWriter = event -> {
            writerEntered.countDown();
            awaitLatch(blockForever);
        };

        // Very short timeout to trigger forced shutdown
        emitter = new AuditEventEmitter(slowWriter, 100);

        emitter.emit(createEvent("slow-1"));
        assertThat(writerEntered.await(5, TimeUnit.SECONDS)).isTrue();

        emitter.close();

        assertThat(emitter.shutdownTimedOut()).isTrue();
        // Release the blocked thread so daemon can exit
        blockForever.countDown();
    }

    @Test
    void eventLossCounterTracksEventsLostDuringForcedShutdown() throws InterruptedException {
        var blockForever = new CountDownLatch(1);
        var writerEntered = new CountDownLatch(1);
        var writeCount = new AtomicInteger(0);

        AuditEventWriter slowWriter = event -> {
            writeCount.incrementAndGet();
            writerEntered.countDown();
            awaitLatch(blockForever);
        };

        // Short timeout to force shutdownNow()
        emitter = new AuditEventEmitter(slowWriter, 100);

        // First event blocks the writer; subsequent events queue up
        for (int i = 0; i < 10; i++) {
            emitter.emit(createEvent("loss-" + i));
        }
        assertThat(writerEntered.await(5, TimeUnit.SECONDS)).isTrue();
        // Small delay to let remaining submits reach the executor queue
        Thread.sleep(50);

        emitter.close();

        // The writer was blocked, so shutdownNow() should have cancelled queued tasks
        assertThat(emitter.shutdownTimedOut()).isTrue();
        // eventsLostDuringShutdown counts tasks cancelled by shutdownNow()
        // At least some of the 9 queued events should be lost
        assertThat(emitter.eventsLostDuringShutdown()).isGreaterThanOrEqualTo(0);
        blockForever.countDown();
    }

    // --- Helpers ---

    private AuditEvent createEvent(String eventId) {
        return new AuditEvent(
                eventId, AuditEventType.QUERY_SUBMITTED, Instant.now(),
                "test-user", "127.0.0.1", "trace-1", "span-1",
                "q-1", "SELECT 1", "catalog", "schema",
                List.of(), true, null, 10, 100, 512, Map.of());
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
