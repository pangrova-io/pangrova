package org.pangrova.observability.sdk.logging.provider.mdc;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.pangrova.spi.observability.Context;

/**
 * Propagates MDC context from the submitting thread to async executing threads.
 * Ensures trace_id, query_id, and other MDC fields are available in
 * thread-pool-executed tasks.
 *
 * <p>Usage: wrap tasks before submitting to an executor:
 * <pre>{@code
 * executor.submit(MdcContextPropagator.wrap(task));
 * }</pre>
 *
 * <p>For the full unified API (Callable, Executor, ExecutorService wrapping),
 * use {@link Context#taskWrapping()} directly.
 *
 * <p>Thread safety: captures MDC snapshot at decoration time (immutable copy),
 * restores on the target thread before execution, and cleans up after execution
 * to prevent MDC leakage in pooled threads.
 */
public final class MdcContextPropagator {

    private static final MdcTaskWrapping DELEGATE = new MdcTaskWrapping();

    private MdcContextPropagator() {}

    /**
     * Wraps a {@link Runnable} to propagate the current thread's MDC context.
     *
     * @param task the task to decorate
     * @return a new Runnable that restores MDC before executing the original task
     */
    public static Runnable wrap(Runnable task) {
        return DELEGATE.wrap(task);
    }

    /**
     * Wraps a {@link Callable} to propagate the current thread's MDC context.
     *
     * @param task the callable to decorate
     * @param <T> the return type of the callable
     * @return a new Callable that restores MDC before executing the original callable
     */
    public static <T> Callable<T> wrap(Callable<T> task) {
        return DELEGATE.wrap(task);
    }

    /**
     * Returns an {@link Executor} that automatically propagates MDC context
     * for every task submitted to the delegate executor.
     *
     * @param delegate the underlying executor
     * @return a context-propagating executor
     */
    public static Executor wrapExecutor(Executor delegate) {
        return DELEGATE.wrapExecutor(delegate);
    }

    /**
     * Returns an {@link ExecutorService} that automatically propagates MDC context
     * for every task submitted to the delegate service.
     *
     * @param delegate the underlying executor service
     * @return a context-propagating executor service
     */
    public static ExecutorService wrapExecutorService(ExecutorService delegate) {
        return DELEGATE.wrapExecutorService(delegate);
    }
}
