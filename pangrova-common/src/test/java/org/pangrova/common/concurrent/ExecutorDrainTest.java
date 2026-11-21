package org.pangrova.common.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Actual thread ownership boundaries only; does not simulate task execution or JNI success. */
class ExecutorDrainTest {
    @Test void normalCloseWaitsForActualFinallyExit() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        executor.execute(() -> ExecutorDrain.runOwned(executor, () -> held(entered, release, exited)));
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            release.countDown();
            ExecutorDrain.shutdownAndAwait(executor, 5000);
        } finally {
            release.countDown();
            ExecutorDrain.shutdownAndAwait(executor, 5000);
        }
        assertEquals(0, exited.getCount());
        assertTrue(executor.isTerminated());
    }

    @Test void timeoutKeepsOwnerAndRetryObservesActualThreadExit() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        executor.execute(() -> ExecutorDrain.runOwned(executor, () -> held(entered, release, exited)));
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, () -> ExecutorDrain.shutdownAndAwait(executor, 10));
            assertFalse(executor.isTerminated());
            assertEquals(1, exited.getCount());
        } finally {
            release.countDown();
            ExecutorDrain.shutdownAndAwait(executor, 5000);
        }
        assertEquals(0, exited.getCount());
    }

    @Test void interruptedJoinRestoresInterruptAndDoesNotClaimDrain() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        executor.execute(() -> ExecutorDrain.runOwned(executor, () -> held(entered, release, exited)));
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            Thread.currentThread().interrupt();
            assertThrows(IllegalStateException.class, () -> ExecutorDrain.shutdownAndAwait(executor, 5000));
            assertTrue(Thread.currentThread().isInterrupted());
            assertFalse(executor.isTerminated());
        } finally {
            Thread.interrupted();
            release.countDown();
            ExecutorDrain.shutdownAndAwait(executor, 5000);
        }
        assertEquals(0, exited.getCount());
    }

    @Test void enteredOwnerRejectsSelfJoinBeforeWaitingButExternalJoinSucceeds() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        var failure = new AtomicReference<Throwable>();
        var exited = new CountDownLatch(1);
        executor.execute(() -> ExecutorDrain.runOwned(executor, () -> {
            try { ExecutorDrain.shutdownAndAwait(executor, 5000); }
            catch (Throwable error) { failure.set(error); }
            finally { exited.countDown(); }
        }));
        assertTrue(exited.await(5, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, failure.get());
        assertTrue(failure.get().getMessage().contains("own owner"));
        ExecutorDrain.shutdownAndAwait(executor, 5000);
        assertTrue(executor.isTerminated());
    }

    @Test void cancellingFutureIsNotAnEnteredCallbackExit() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        var future = executor.submit(() -> ExecutorDrain.runOwned(executor, () -> held(entered, release, exited)));
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertTrue(future.cancel(false));
            assertTrue(future.isDone());
            assertEquals(1, exited.getCount());
            assertThrows(IllegalStateException.class, () -> ExecutorDrain.shutdownAndAwait(executor, 10));
        } finally {
            release.countDown();
            ExecutorDrain.shutdownAndAwait(executor, 5000);
        }
        assertEquals(0, exited.getCount());
    }

    private static void held(CountDownLatch entered, CountDownLatch release, CountDownLatch exited) {
        entered.countDown();
        try {
            if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Actual owner release not signalled");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        } finally { exited.countDown(); }
    }

    @Test void nullArgumentsAreRejectedBeforeExecutorAdmissionChanges() {
        var executor = Executors.newSingleThreadExecutor();
        try {
            assertThrows(NullPointerException.class, () -> ExecutorDrain.runOwned(null, () -> { }));
            assertThrows(NullPointerException.class, () -> ExecutorDrain.runOwned(executor, null));
            assertThrows(NullPointerException.class, () -> ExecutorDrain.shutdownAndAwait(null, 1));
            assertThrows(IllegalArgumentException.class, () -> ExecutorDrain.shutdownAndAwait(executor, 0));
            assertFalse(executor.isShutdown());
        } finally { ExecutorDrain.shutdownAndAwait(executor, 5000); }
    }
}
