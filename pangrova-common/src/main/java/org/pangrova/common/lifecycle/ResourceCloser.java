package org.pangrova.common.lifecycle;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Static utility methods for safely closing resources and shutting down executors.
 *
 * <p>Use {@link #closeAll(Throwable, Runnable...)} when callers must observe failures.
 * Use {@link #present(Object, Consumer)} for partially initialized resources.
 *
 * @see java.lang.AutoCloseable
 * @see java.util.concurrent.ExecutorService
 */
public final class ResourceCloser {

    private ResourceCloser() {}

    /** Skips resources whose acquisition did not complete. */
    public static <T> Runnable present(T resource, Consumer<? super T> release) {
        return () -> {
            if (resource != null) release.accept(resource);
        };
    }

    /** Releases in caller-specified order and suppresses secondary failures onto the primary. */
    public static void closeAll(Throwable primary, Runnable... releases) {
        Throwable failure = primary;
        for (Runnable release : releases) {
            try {
                release.run();
            } catch (RuntimeException | Error cleanupFailure) {
                if (failure == null) {
                    failure = cleanupFailure;
                } else if (failure != cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (primary != null) return;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }

    /** Releases an acquired resource when initialization fails, retaining both failures. */
    public static void closeOnFailure(Throwable failure, AutoCloseable resource) {
        try {
            resource.close();
        } catch (Exception | Error cleanup) {
            if (cleanup != failure) failure.addSuppressed(cleanup);
        }
    }

    /**
     * Closes the given resource, silently swallowing any exception.
     *
     * @param resource the resource to close, may be {@code null}
     */
    public static void closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception ignored) {
            // intentionally swallowed
        }
    }

    /**
     * Closes the given resource, logging any exception at WARNING level.
     *
     * @param resource the resource to close, may be {@code null}
     * @param log      the logger to use for failure reporting, may be {@code null}
     * @param label    a human-readable label identifying the resource
     */
    public static void closeQuietly(AutoCloseable resource, Logger log, String label) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception e) {
            if (log != null) {
                log.log(Level.WARNING,
                        String.format("event=close_failed label=%s error=%s", label, e.getMessage()));
            }
        }
    }

    /**
     * Gracefully shuts down an executor service without logging.
     *
     * <p>Sequence: {@code shutdown()} → {@code awaitTermination(timeout)} →
     * {@code shutdownNow()} if timeout expires. If interrupted during
     * {@code awaitTermination}, calls {@code shutdownNow()} and restores
     * the interrupt flag.
     *
     * @param executor the executor to shut down, may be {@code null}
     * @param timeout  the maximum time to wait for termination
     */
    public static void shutdownExecutor(ExecutorService executor, Duration timeout) {
        shutdownExecutor(executor, timeout, null, null);
    }

    /**
     * Gracefully shuts down an executor service with optional logging.
     *
     * <p>Sequence: {@code shutdown()} → {@code awaitTermination(timeout)} →
     * {@code shutdownNow()} if timeout expires. If interrupted during
     * {@code awaitTermination}, calls {@code shutdownNow()} and restores
     * the interrupt flag.
     *
     * @param executor the executor to shut down, may be {@code null}
     * @param timeout  the maximum time to wait for termination
     * @param name     a human-readable name for the executor (used in log messages)
     * @param log      the logger to use, may be {@code null}
     */
    public static void shutdownExecutor(ExecutorService executor, Duration timeout,
                                        String name, Logger log) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                forceShutdown(executor, name, log);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void forceShutdown(ExecutorService executor, String name, Logger log) {
        executor.shutdownNow();
        if (log != null) {
            log.log(Level.WARNING,
                    String.format("event=executor_shutdown_forced name=%s", name));
        }
    }
}
