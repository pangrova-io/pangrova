package org.pangrova.coordinator.commit;

import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.index.CommitListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ResilientCommitListenerDispatcher}.
 * Verifies queuing, retry, overflow, and observability counter behavior.
 */
class ResilientCommitListenerDispatcherTest {

    private ResilientCommitListenerDispatcher dispatcher;

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    private static CommitEvent event(String table) {
        return new CommitEvent("catalog", "schema", table,
                List.of("file1.parquet"), List.of(),
                Instant.now(), "1", CommitEventType.APPEND);
    }

    @Test
    void successfulDispatchIncrementsCounter() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CopyOnWriteArrayList<CommitEvent> received = new CopyOnWriteArrayList<>();
        dispatcher = new ResilientCommitListenerDispatcher(e -> {
            received.add(e);
            latch.countDown();
        });

        dispatcher.dispatch(event("t1"));
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Event should be delivered");
        assertEquals(1, received.size());
        assertEquals("t1", received.get(0).tableName());

        awaitCondition(() -> dispatcher.dispatchedCount() == 1, 2000);
        assertEquals(1, dispatcher.dispatchedCount());
        assertEquals(0, dispatcher.droppedCount());
        assertEquals(0, dispatcher.failedCount());
    }

    @Test
    void retryOnTransientFailure() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        dispatcher = new ResilientCommitListenerDispatcher(e -> {
            int n = callCount.incrementAndGet();
            if (n <= 2) {
                throw new RuntimeException("transient failure #" + n);
            }
            latch.countDown();
        }, 16, 3, 10, 100);

        dispatcher.dispatch(event("t1"));
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Should succeed after retries");

        awaitCondition(() -> dispatcher.dispatchedCount() == 1, 2000);
        assertEquals(1, dispatcher.dispatchedCount());
        assertEquals(0, dispatcher.failedCount());
        assertEquals(2, dispatcher.retriedCount());
    }

    @Test
    void permanentFailureAfterMaxRetries() throws Exception {
        dispatcher = new ResilientCommitListenerDispatcher(e -> {
            throw new RuntimeException("permanent failure");
        }, 16, 2, 10, 100);

        dispatcher.dispatch(event("t1"));

        awaitCondition(() -> dispatcher.failedCount() == 1, 5000);
        assertEquals(1, dispatcher.failedCount());
        assertEquals(0, dispatcher.dispatchedCount());
        assertEquals(2, dispatcher.retriedCount());
    }

    @Test
    void overflowDropsOldestEvent() throws Exception {
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch firstEventTaken = new CountDownLatch(1);

        dispatcher = new ResilientCommitListenerDispatcher(e -> {
            firstEventTaken.countDown();
            try {
                blockLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, 2, 0, 10, 100);

        dispatcher.dispatch(event("first"));
        assertTrue(firstEventTaken.await(5, TimeUnit.SECONDS),
                "Dispatch thread should take first event");

        dispatcher.dispatch(event("second"));
        dispatcher.dispatch(event("third"));
        dispatcher.dispatch(event("fourth"));

        assertTrue(dispatcher.droppedCount() >= 1,
                "At least one event should be dropped");

        blockLatch.countDown();
    }

    @Test
    void dispatchAfterCloseDrops() throws Exception {
        CopyOnWriteArrayList<CommitEvent> received = new CopyOnWriteArrayList<>();
        dispatcher = new ResilientCommitListenerDispatcher(received::add);
        dispatcher.close();

        dispatcher.dispatch(event("t1"));
        assertEquals(1, dispatcher.droppedCount());
        Thread.sleep(200);
        assertEquals(0, received.size());
    }

    @Test
    void asListenerDelegatesToDispatch() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CopyOnWriteArrayList<CommitEvent> received = new CopyOnWriteArrayList<>();
        dispatcher = new ResilientCommitListenerDispatcher(e -> {
            received.add(e);
            latch.countDown();
        });

        CommitListener listener = dispatcher.asListener();
        listener.onCommit(event("t1"));
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, received.size());
    }

    @Test
    void constructorValidation() {
        CommitListener noop = e -> {};
        assertThrows(NullPointerException.class,
                () -> new ResilientCommitListenerDispatcher(null));
        assertThrows(IllegalArgumentException.class,
                () -> new ResilientCommitListenerDispatcher(noop, 0, 3, 50, 2000));
        assertThrows(IllegalArgumentException.class,
                () -> new ResilientCommitListenerDispatcher(noop, 10, -1, 50, 2000));
        assertThrows(IllegalArgumentException.class,
                () -> new ResilientCommitListenerDispatcher(noop, 10, 3, 0, 2000));
        assertThrows(IllegalArgumentException.class,
                () -> new ResilientCommitListenerDispatcher(noop, 10, 3, 100, 50));
    }

    @Test
    void multipleEventsDeliveredInOrder() throws Exception {
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);
        dispatcher = new ResilientCommitListenerDispatcher(e -> {
            received.add(e.tableName());
            latch.countDown();
        });

        dispatcher.dispatch(event("a"));
        dispatcher.dispatch(event("b"));
        dispatcher.dispatch(event("c"));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("a", "b", "c"), received);
        awaitCondition(() -> dispatcher.dispatchedCount() == 3, 2000);
        assertEquals(3, dispatcher.dispatchedCount());
    }

    @Test
    void pendingCountReflectsQueueState() throws Exception {
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch firstTaken = new CountDownLatch(1);

        dispatcher = new ResilientCommitListenerDispatcher(e -> {
            firstTaken.countDown();
            try {
                blockLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, 16, 0, 10, 100);

        dispatcher.dispatch(event("first"));
        assertTrue(firstTaken.await(5, TimeUnit.SECONDS));

        dispatcher.dispatch(event("second"));
        dispatcher.dispatch(event("third"));
        assertTrue(dispatcher.pendingCount() >= 2,
                "At least 2 events should be pending");

        blockLatch.countDown();
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition,
                                       long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Condition not met within " + timeoutMs + "ms");
            }
            Thread.sleep(50);
        }
    }
}
