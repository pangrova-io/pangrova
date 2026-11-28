package org.pangrova.common.concurrency;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Centralized factory for managed executor services.
 * All daemon thread pools in Pangrova should be created through this factory
 * to ensure consistent naming, daemon status, and future observability hooks.
 */
public final class ManagedExecutorFactory {

    private ManagedExecutorFactory() {}

    public static ExecutorService newSingleThreadExecutor(String name) {
        return Executors.newSingleThreadExecutor(daemonThreadFactory(name));
    }

    public static ExecutorService newFixedThreadPool(String name, int threads) {
        return Executors.newFixedThreadPool(threads, daemonThreadFactory(name));
    }

    /** Bounded work queue; overload is rejected instead of retaining unbounded queries. */
    public static ExecutorService newBoundedThreadPool(
            String name, int threads, int queueCapacity) {
        if (threads <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException("threads and queueCapacity must be positive");
        }
        return new ThreadPoolExecutor(
                threads, threads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), daemonThreadFactory(name),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String name) {
        var stpe = new java.util.concurrent.ScheduledThreadPoolExecutor(1, daemonThreadFactory(name));
        stpe.setRemoveOnCancelPolicy(true);
        return stpe;
    }

    /**
     * Creates a scheduled thread pool with the given name and thread count.
     * Automatically sets {@code removeOnCancelPolicy(true)} for governance compliance.
     *
     * @param name    the thread name prefix
     * @param threads the number of threads in the pool
     * @return a new {@link ScheduledExecutorService} with removeOnCancelPolicy enabled
     */
    public static ScheduledExecutorService newScheduledThreadPool(String name, int threads) {
        var exec = new java.util.concurrent.ScheduledThreadPoolExecutor(threads, daemonThreadFactory(name));
        exec.setRemoveOnCancelPolicy(true);
        return exec;
    }

    private static ThreadFactory daemonThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger(0);
        return r -> {
            Thread t = new Thread(r, prefix + "-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }
}
