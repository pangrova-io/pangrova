package org.pangrova.governance.lineage;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.spi.execution.QueryState;
import org.pangrova.spi.execution.eventlistener.QueryCompletedEvent;
import org.pangrova.spi.execution.eventlistener.QueryCreatedEvent;
import org.pangrova.spi.execution.eventlistener.QueryStatistics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link LineageQueryEventListener} — verifying the EventListener SPI
 * to lineage bridge (Task 182).
 */
class LineageQueryEventListenerTest {

    @Test
    void queryCompletedSuccessEmitsCompleteEvent() throws InterruptedException {
        var backend = new CountingBackend(1);
        var emitter = new LineageEventEmitter(backend);
        var listener = new LineageQueryEventListener(emitter);

        listener.queryCompleted(new QueryCompletedEvent(
                "q1", "SELECT 1", "alice", QueryState.FINISHED,
                1000, 1100, QueryStatistics.wallTimeOnly(100), Optional.empty()));

        backend.awaitEvents();
        assertThat(backend.completeCount.get()).isEqualTo(1);
        assertThat(backend.failCount.get()).isEqualTo(0);
        emitter.close();
    }

    @Test
    void queryCompletedFailureEmitsFailEvent() throws InterruptedException {
        var backend = new CountingBackend(1);
        var emitter = new LineageEventEmitter(backend);
        var listener = new LineageQueryEventListener(emitter);

        listener.queryCompleted(new QueryCompletedEvent(
                "q2", "SELECT bad", "bob", QueryState.FAILED,
                2000, 2050, QueryStatistics.wallTimeOnly(50),
                Optional.of("table not found")));

        backend.awaitEvents();
        assertThat(backend.failCount.get()).isEqualTo(1);
        assertThat(backend.completeCount.get()).isEqualTo(0);
        emitter.close();
    }

    @Test
    void queryCreatedDoesNotEmitLineage() throws InterruptedException {
        var backend = new CountingBackend(0);
        var emitter = new LineageEventEmitter(backend);
        var listener = new LineageQueryEventListener(emitter);

        listener.queryCreated(new QueryCreatedEvent("q3", "SELECT 1", "carol", 3000));

        // Give the async consumer a moment to process (should see nothing)
        Thread.sleep(200);
        assertThat(backend.totalCount.get()).isEqualTo(0);
        emitter.close();
    }

    /** Simple backend that counts events by type with a latch for synchronization. */
    private static final class CountingBackend implements LineageBackend {
        final AtomicInteger completeCount = new AtomicInteger();
        final AtomicInteger failCount = new AtomicInteger();
        final AtomicInteger totalCount = new AtomicInteger();
        private final CountDownLatch latch;

        CountingBackend(int expectedEvents) {
            this.latch = new CountDownLatch(expectedEvents);
        }

        @Override
        public void send(LineageEvent event) {
            totalCount.incrementAndGet();
            switch (event.eventType()) {
                case COMPLETE -> completeCount.incrementAndGet();
                case FAIL -> failCount.incrementAndGet();
                default -> {}
            }
            latch.countDown();
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        void awaitEvents() throws InterruptedException {
            assertThat(latch.await(5, TimeUnit.SECONDS))
                    .as("Expected events should arrive within 5 seconds")
                    .isTrue();
        }
    }
}
