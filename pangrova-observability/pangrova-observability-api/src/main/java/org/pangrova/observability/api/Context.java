package org.pangrova.observability.api;

import org.pangrova.common.api.annotation.Experimental;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * Opaque trace context carrier.
 *
 * <p>Implementations hold propagation state (trace-id, span-id, etc.)
 * and are threaded through {@link TracingProvider#inject} /
 * {@link TracingProvider#extract} for cross-process context propagation.
 *
 * <p>This is Pangrova's own lightweight abstraction, independent of any
 * specific tracing SDK such as OpenTelemetry.
 *
 * <p>For async MDC propagation, use {@link #taskWrapping()} to obtain a
 * {@link TaskWrapping} instance that captures and restores MDC context
 * (trace_id, queryId, taskId) across thread boundaries.
 */
@Experimental
public interface Context {

    /**
     * Returns the singleton {@link TaskWrapping} instance for propagating MDC context
     * across async thread boundaries.
     *
     * <p>Usage examples:
     * <pre>{@code
     * // Wrap a Runnable
     * executor.submit(Context.taskWrapping().wrap(myRunnable));
     *
     * // Wrap a Callable
     * Future<T> f = executor.submit(Context.taskWrapping().wrap(myCallable));
     *
     * // Decorate an entire Executor
     * Executor wrapped = Context.taskWrapping().wrapExecutor(rawExecutor);
     *
     * // Decorate an ExecutorService
     * ExecutorService wrapped = Context.taskWrapping().wrapExecutorService(rawService);
     * }</pre>
     *
     * @return the task wrapping utility, never null
     */
    static TaskWrapping taskWrapping() {
        return TaskWrappingLoader.INSTANCE;
    }

    /**
     * Provides MDC context propagation utilities for async task execution.
     *
     * <p>Captures the current thread's MDC state (trace_id, queryId, taskId) at
     * decoration time and restores it on the executing thread before task execution.
     * After execution completes, the MDC is cleaned up to prevent leakage in pooled threads.
     */
    interface TaskWrapping {

        /**
         * Wraps a {@link Runnable} to propagate the current thread's MDC context.
         *
         * @param task the task to decorate, must not be null
         * @return a new Runnable that restores MDC before executing the original task
         */
        Runnable wrap(Runnable task);

        /**
         * Wraps a {@link Callable} to propagate the current thread's MDC context.
         *
         * @param task the callable to decorate, must not be null
         * @param <T> the return type of the callable
         * @return a new Callable that restores MDC before executing the original callable
         */
        <T> Callable<T> wrap(Callable<T> task);

        /**
         * Returns an {@link Executor} that automatically propagates MDC context
         * for every task submitted to the delegate executor.
         *
         * @param delegate the underlying executor, must not be null
         * @return a context-propagating executor
         */
        Executor wrapExecutor(Executor delegate);

        /**
         * Returns an {@link ExecutorService} that automatically propagates MDC context
         * for every task submitted to the delegate service.
         *
         * @param delegate the underlying executor service, must not be null
         * @return a context-propagating executor service
         */
        ExecutorService wrapExecutorService(ExecutorService delegate);
    }
}
