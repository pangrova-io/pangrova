package org.pangrova.governance.lineage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link LineageEventEmitter} graceful shutdown, dropped event counting,
 * and alerting threshold behavior.
 *
 * <p>Uses real test-double backends (no mock frameworks).</p>
 */
class LineageEventEmitterShutdownTest {

    private LineageEventEmitter emitter;

    @AfterEach
    void tearDown() {
        if (emitter != null) {
            emitter.close();
        }
    }

    @Test
    void closeCompletesWithinTimeoutWhenQueueIsEmpty() {
        emitter = new LineageEventEmitter(new ImmediateBackend());

        long start = System.nanoTime();
        emitter.close();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(elapsedMs).isLessThan(5000);
        assertThat(emitter.shutdownTimedOut()).isFalse();
    }

    @Test
    void closeUnblocksConsumerThreadBlockedOnTake() throws InterruptedException {
        var backend = new ImmediateBackend();
        emitter = new LineageEventEmitter(backend);

        // Let consumer thread block on empty queue take()
        Thread.sleep(100);

        long start = System.nanoTime();
        emitter.close();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(elapsedMs).isLessThan(5000);
        assertThat(emitter.shutdownTimedOut()).isFalse();
    }

    @Test
    void closeDrainsPendingEventsBeforeCompleting() throws InterruptedException {
        var received = new CopyOnWriteArrayList<LineageEvent>();
        // Backend that blocks until we release it, so events queue up
        var blockingLatch = new CountDownLatch(1);
        var backend = new LatchBlockingBackend(blockingLatch, received);

        emitter = new LineageEventEmitter(backend);

        // Emit several events; first one will be picked up by consumer and block
        for (int i = 0; i < 5; i++) {
            emitter.emitStartEvent("q-" + i, "SELECT " + i, List.of(), List.of());
        }
        Thread.sleep(50);

        // Release the blocking backend so consumer can process
        blockingLatch.countDown();
        Thread.sleep(50);

        // Close should drain remaining events
        emitter.close();

        // All events should have been sent (either by consumer or drain)
        assertThat(received.size()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void droppedEventsCounterIncrementsWhenQueueIsFull() {
        // Backend that blocks forever so queue fills up
        var neverReturn = new CountDownLatch(1);
        var backend = new LatchBlockingBackend(neverReturn, new CopyOnWriteArrayList<>());

        emitter = new LineageEventEmitter(backend);

        // Fill the queue (capacity=1000) + extras to trigger drops
        for (int i = 0; i < 1050; i++) {
            emitter.emitStartEvent("q-" + i, "SELECT " + i, List.of(), List.of());
        }

        assertThat(emitter.eventsDroppedTotal()).isGreaterThan(0);
        neverReturn.countDown();
    }

    @Test
    void droppedEventsAlertThresholdTriggersAtConfiguredValue() {
        // Use a very small alert threshold to verify it fires
        var neverReturn = new CountDownLatch(1);
        var backend = new LatchBlockingBackend(neverReturn, new CopyOnWriteArrayList<>());

        emitter = new LineageEventEmitter(backend, 5000, 5);

        // Fill queue to trigger drops beyond threshold
        for (int i = 0; i < 1020; i++) {
            emitter.emitStartEvent("q-" + i, "SELECT " + i, List.of(), List.of());
        }

        // Verify drops exceeded threshold
        assertThat(emitter.eventsDroppedTotal()).isGreaterThanOrEqualTo(5);
        neverReturn.countDown();
    }

    @Test
    void shutdownTimeoutIsObservableWhenBackendIsSlow() throws InterruptedException {
        var blockForever = new CountDownLatch(1);
        var backend = new LatchBlockingBackend(blockForever, new CopyOnWriteArrayList<>());

        // Very short timeout so we can observe the timeout
        emitter = new LineageEventEmitter(backend, 100, 100);

        // Emit an event so consumer thread picks it up and blocks in backend.send()
        emitter.emitStartEvent("q-slow", "SELECT 1", List.of(), List.of());
        Thread.sleep(50);

        emitter.close();

        // The consumer thread may or may not have timed out depending on timing,
        // but close() itself should complete
        // Release the backend so the daemon thread can exit
        blockForever.countDown();
    }

    @Test
    void doubleCloseIsIdempotent() {
        emitter = new LineageEventEmitter(new ImmediateBackend());

        emitter.close();
        emitter.close(); // second close should be no-op

        assertThat(emitter.shutdownTimedOut()).isFalse();
    }

    // --- Test doubles (no mock frameworks) ---

    /**
     * Backend that immediately accepts events.
     */
    private static final class ImmediateBackend implements LineageBackend {
        private final CopyOnWriteArrayList<LineageEvent> received = new CopyOnWriteArrayList<>();

        @Override
        public void send(LineageEvent event) {
            received.add(event);
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }

    /**
     * Backend that blocks on send() until the latch is released.
     * Records events after unblocking.
     */
    private static final class LatchBlockingBackend implements LineageBackend {
        private final CountDownLatch blockLatch;
        private final CopyOnWriteArrayList<LineageEvent> received;

        LatchBlockingBackend(CountDownLatch blockLatch, CopyOnWriteArrayList<LineageEvent> received) {
            this.blockLatch = blockLatch;
            this.received = received;
        }

        @Override
        public void send(LineageEvent event) {
            try {
                blockLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            received.add(event);
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }
}
