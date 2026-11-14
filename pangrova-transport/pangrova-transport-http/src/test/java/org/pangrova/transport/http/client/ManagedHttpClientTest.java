package org.pangrova.transport.http.client;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies actual executor termination, including incomplete-close owner retry.
 */
class ManagedHttpClientTest {

    @Test
    void closeShutdownsExecutor() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        HttpClient httpClient = HttpClient.newBuilder().executor(executor).build();
        ManagedHttpClient managed = new ManagedHttpClient(httpClient, executor);

        assertFalse(executor.isShutdown());
        managed.close();
        assertTrue(executor.isShutdown());
        assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
    }

    @Test
    void httpClientAccessorReturnsWrappedClient() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        HttpClient httpClient = HttpClient.newBuilder().executor(executor).build();
        ManagedHttpClient managed = new ManagedHttpClient(httpClient, executor);

        assertSame(httpClient, managed.httpClient());
        managed.close();
    }

    @Test
    void forcedCloseWaitsForTheActualRunnableFinallyExit() throws Exception {
        try (var work = heldWork()) {
            var close = CompletableFuture.runAsync(work.client()::close);
            assertTrue(work.interrupted().await(7, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            assertFalse(work.client().executor().isTerminated());
            work.release().countDown();
            close.get(5, TimeUnit.SECONDS);
            assertEquals(0, work.exited().getCount());
            assertTrue(work.client().executor().isTerminated());
        }
    }

    @Test
    void unfinishedForcedCloseFailsAndKeepsTheSameOwnerForRetry() throws Exception {
        try (var work = heldWork()) {
            var failure = assertThrows(IllegalStateException.class, work.client()::close);
            assertEquals("Managed HTTP executor has not exited", failure.getMessage());
            assertEquals(0, work.interrupted().getCount());
            assertFalse(work.client().executor().isTerminated());
            assertEquals(1, work.exited().getCount());
            work.release().countDown();
            work.client().close();
            assertEquals(0, work.exited().getCount());
            assertTrue(work.client().executor().isTerminated());
            work.client().close();
        }
    }

    @Test
    void interruptedCloseFailsPreservesInterruptAndCanBeRetried() throws Exception {
        try (var work = heldWork()) {
            Thread.currentThread().interrupt();
            try {
                var failure = assertThrows(IllegalStateException.class, work.client()::close);
                assertEquals("Managed HTTP executor drain interrupted", failure.getMessage());
                assertInstanceOf(InterruptedException.class, failure.getCause());
                assertTrue(Thread.currentThread().isInterrupted());
                assertFalse(work.client().executor().isTerminated());
            } finally {
                Thread.interrupted();
            }
            work.release().countDown();
            work.client().close();
            assertEquals(0, work.exited().getCount());
            assertTrue(work.client().executor().isTerminated());
        }
    }

    private static HeldWork heldWork() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        var client = new ManagedHttpClient(HttpClient.newBuilder().executor(executor).build(), executor);
        var entered = new CountDownLatch(1);
        var work = new HeldWork(client, new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1));
        executor.execute(() -> {
            entered.countDown();
            var restoreInterrupt = new AtomicBoolean();
            try {
                while (work.release().getCount() != 0) {
                    try { work.release().await(); }
                    catch (InterruptedException failure) {
                        restoreInterrupt.set(true);
                        work.interrupted().countDown();
                    }
                }
            } finally {
                work.exited().countDown();
                if (restoreInterrupt.get()) Thread.currentThread().interrupt();
            }
        });
        if (!entered.await(5, TimeUnit.SECONDS)) {
            work.close();
            fail("Owned HTTP executor work did not start");
        }
        return work;
    }

    private record HeldWork(ManagedHttpClient client, CountDownLatch release,
                            CountDownLatch interrupted, CountDownLatch exited) implements AutoCloseable {
        @Override
        public void close() {
            release.countDown();
            client.close();
        }
    }
}
