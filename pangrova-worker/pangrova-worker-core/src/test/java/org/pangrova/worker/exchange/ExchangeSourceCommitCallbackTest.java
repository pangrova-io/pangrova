package org.pangrova.worker.exchange;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates the CompletableFuture-based commit callback mechanism in
 * ExchangeSourceRegistry. Ensures exchange sources block on a future
 * and unblock within 1 second when upstream commits.
 */
class ExchangeSourceCommitCallbackTest {

    private ExchangeSourceRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ExchangeSourceRegistry();
    }

    @Test
    void exchangeSourceBlocks_upstreamCommits_sourceUnblocksWithin1Second() throws Exception {
        String queryId = "q-callback-1";
        int stageId = 3;

        AtomicBoolean unblocked = new AtomicBoolean(false);
        CountDownLatch waitingStarted = new CountDownLatch(1);

        CompletableFuture<Void> commitFuture = registry.waitForCommit(queryId, stageId, Duration.ofSeconds(10));

        // Start a thread that blocks on the future
        Thread waiter = new Thread(() -> {
            waitingStarted.countDown();
            try {
                commitFuture.get(10, TimeUnit.SECONDS);
                unblocked.set(true);
            } catch (Exception e) {
                // Should not happen
            }
        });
        waiter.start();

        // Ensure the waiter thread is actually waiting
        assertTrue(waitingStarted.await(1, TimeUnit.SECONDS));
        assertFalse(unblocked.get(), "Source should still be blocked before commit");

        // Simulate upstream commit
        long beforeCommit = System.nanoTime();
        registry.notifyCommit(queryId, stageId);

        // Wait for the waiter to unblock
        waiter.join(2000);
        long elapsed = System.nanoTime() - beforeCommit;

        assertTrue(unblocked.get(), "Source must unblock after upstream commit");
        assertTrue(Duration.ofNanos(elapsed).toMillis() < 1000,
                "Source must unblock within 1 second, took " + Duration.ofNanos(elapsed).toMillis() + "ms");
    }

    @Test
    void waitForCommit_alreadyCommitted_returnsImmediately() throws Exception {
        String queryId = "q-already";
        int stageId = 1;

        registry.notifyCommit(queryId, stageId);

        CompletableFuture<Void> future = registry.waitForCommit(queryId, stageId, Duration.ofSeconds(1));

        assertTrue(future.isDone(), "Future should be immediately done for already-committed upstream");
        assertNull(future.get(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void waitForCommit_timeout_completesExceptionally() {
        String queryId = "q-timeout";
        int stageId = 2;

        CompletableFuture<Void> future = registry.waitForCommit(queryId, stageId, Duration.ofMillis(100));

        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, ex.getCause());
    }

    @Test
    void multipleWaiters_sameUpstream_allUnblock() throws Exception {
        String queryId = "q-multi";
        int stageId = 5;

        CompletableFuture<Void> future1 = registry.waitForCommit(queryId, stageId, Duration.ofSeconds(5));
        CompletableFuture<Void> future2 = registry.waitForCommit(queryId, stageId, Duration.ofSeconds(5));

        assertFalse(future1.isDone());
        assertFalse(future2.isDone());

        registry.notifyCommit(queryId, stageId);

        assertNull(future1.get(1, TimeUnit.SECONDS));
        assertNull(future2.get(1, TimeUnit.SECONDS));
    }

    @Test
    void isCommitted_beforeAndAfterNotify() {
        String queryId = "q-check";
        int stageId = 7;

        assertFalse(registry.isCommitted(queryId, stageId));
        registry.notifyCommit(queryId, stageId);
        assertTrue(registry.isCommitted(queryId, stageId));
    }

    @Test
    void cleanup_cancelsPendingFutures() {
        String queryId = "q-cleanup";
        int stageId = 4;

        CompletableFuture<Void> future = registry.waitForCommit(queryId, stageId, Duration.ofSeconds(30));
        assertFalse(future.isDone());

        registry.cleanup(queryId);

        assertTrue(future.isCancelled(), "Pending future should be cancelled on cleanup");
    }

    @Test
    void cleanup_removesCommittedState() {
        String queryId = "q-clean-state";
        int stageId = 1;

        registry.notifyCommit(queryId, stageId);
        assertTrue(registry.isCommitted(queryId, stageId));

        registry.cleanup(queryId);
        assertFalse(registry.isCommitted(queryId, stageId));
    }
}
