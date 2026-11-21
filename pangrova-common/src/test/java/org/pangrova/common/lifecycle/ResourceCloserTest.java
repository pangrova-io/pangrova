package org.pangrova.common.lifecycle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for {@link ResourceCloser}.
 * Covers closeQuietly (normal, exception, null) and shutdownExecutor
 * (normal, timeout-forced, null, interrupt restoration).
 */
class ResourceCloserTest {

    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (executor != null && !executor.isShutdown()) {
            executor.shutdownNow();
        }
    }

    // ── closeQuietly(AutoCloseable) ──

    @Test
    void initializationFailureRetainsCheckedCleanupFailure() {
        var initialization = new IllegalStateException("initialize");
        var cleanup = new java.io.IOException("close");
        ResourceCloser.closeOnFailure(initialization, () -> { throw cleanup; });
        assertThat(initialization.getSuppressed()).containsExactly(cleanup);
    }

    @Test
    void closeQuietly_callsCloseOnResource() {
        var closed = new AtomicBoolean(false);
        AutoCloseable resource = () -> closed.set(true);

        ResourceCloser.closeQuietly(resource);

        assertThat(closed).isTrue();
    }

    @Test
    void closeQuietly_swallowsException() {
        AutoCloseable failing = () -> { throw new RuntimeException("boom"); };

        assertThatCode(() -> ResourceCloser.closeQuietly(failing))
                .doesNotThrowAnyException();
    }

    @Test
    void closeQuietly_nullResource_isNoOp() {
        assertThatCode(() -> ResourceCloser.closeQuietly(null))
                .doesNotThrowAnyException();
    }

    // ── closeQuietly(AutoCloseable, Logger, String) ──

    @Test
    void closeQuietlyWithLogger_callsCloseOnResource() {
        var closed = new AtomicBoolean(false);
        AutoCloseable resource = () -> closed.set(true);
        Logger log = Logger.getLogger("test.closeQuietly.success");

        ResourceCloser.closeQuietly(resource, log, "test-resource");

        assertThat(closed).isTrue();
    }

    @Test
    void closeQuietlyWithLogger_logsWarningOnFailure() {
        AutoCloseable failing = () -> { throw new RuntimeException("disk full"); };
        Logger log = Logger.getLogger("test.closeQuietly.failure");
        log.setUseParentHandlers(false);

        var captured = new LogRecord[1];
        log.addHandler(new Handler() {
            @Override public void publish(LogRecord record) { captured[0] = record; }
            @Override public void flush() {}
            @Override public void close() {}
        });

        ResourceCloser.closeQuietly(failing, log, "my-conn");

        assertThat(captured[0]).isNotNull();
        assertThat(captured[0].getLevel()).isEqualTo(Level.WARNING);
        assertThat(captured[0].getMessage()).contains("close_failed").contains("my-conn").contains("disk full");
    }

    @Test
    void closeQuietlyWithLogger_nullResource_isNoOp() {
        Logger log = Logger.getLogger("test.closeQuietly.null");

        assertThatCode(() -> ResourceCloser.closeQuietly(null, log, "label"))
                .doesNotThrowAnyException();
    }

    @Test
    void closeQuietlyWithLogger_nullLogger_swallowsException() {
        AutoCloseable failing = () -> { throw new RuntimeException("boom"); };

        assertThatCode(() -> ResourceCloser.closeQuietly(failing, null, "label"))
                .doesNotThrowAnyException();
    }

    // ── shutdownExecutor — normal termination ──

    @Test
    void shutdownExecutor_normalTermination() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        var taskRan = new AtomicBoolean(false);
        executor.submit(() -> taskRan.set(true));

        ResourceCloser.shutdownExecutor(executor, Duration.ofSeconds(2));

        assertThat(executor.isShutdown()).isTrue();
        assertThat(executor.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shutdownExecutor_timeoutForcesShutdownNow() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        var started = new CountDownLatch(1);
        executor.submit(() -> {
            started.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        started.await(1, TimeUnit.SECONDS);

        ResourceCloser.shutdownExecutor(executor, Duration.ofMillis(50));

        assertThat(executor.isShutdown()).isTrue();
    }

    @Test
    void shutdownExecutor_timeoutForcesShutdownNow_withLogging() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        var started = new CountDownLatch(1);
        executor.submit(() -> {
            started.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        started.await(1, TimeUnit.SECONDS);

        Logger log = Logger.getLogger("test.shutdownExecutor.forced");
        log.setUseParentHandlers(false);
        var captured = new LogRecord[1];
        log.addHandler(new Handler() {
            @Override public void publish(LogRecord record) { captured[0] = record; }
            @Override public void flush() {}
            @Override public void close() {}
        });

        ResourceCloser.shutdownExecutor(executor, Duration.ofMillis(50), "my-pool", log);

        assertThat(captured[0]).isNotNull();
        assertThat(captured[0].getLevel()).isEqualTo(Level.WARNING);
        assertThat(captured[0].getMessage()).contains("executor_shutdown_forced").contains("my-pool");
    }

    @Test
    void shutdownExecutor_nullExecutor_isNoOp() {
        assertThatCode(() -> ResourceCloser.shutdownExecutor(null, Duration.ofSeconds(1)))
                .doesNotThrowAnyException();
    }

    @Test
    void shutdownExecutor_nullExecutorWithLogger_isNoOp() {
        Logger log = Logger.getLogger("test.shutdownExecutor.null");

        assertThatCode(() -> ResourceCloser.shutdownExecutor(null, Duration.ofSeconds(1), "name", log))
                .doesNotThrowAnyException();
    }

    @Test
    void shutdownExecutor_restoresInterruptFlag() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        var started = new CountDownLatch(1);
        executor.submit(() -> {
            started.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        started.await(1, TimeUnit.SECONDS);

        // Run shutdownExecutor on a thread we can interrupt
        var interruptRestored = new AtomicBoolean(false);
        var shutdownThread = new Thread(() -> {
            Thread.currentThread().interrupt(); // pre-set interrupt flag
            ResourceCloser.shutdownExecutor(executor, Duration.ofSeconds(5));
            interruptRestored.set(Thread.currentThread().isInterrupted());
        });
        shutdownThread.start();
        shutdownThread.join(5_000);

        assertThat(interruptRestored).isTrue();
    }

    @Test
    void cannotInstantiate() throws Exception {
        var ctor = ResourceCloser.class.getDeclaredConstructor();
        assertThat(ctor.canAccess(null)).isFalse();
    }
}
