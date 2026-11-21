package org.pangrova.observability.api.context;

import org.pangrova.common.api.annotation.Experimental;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/** Captures context when decorating tasks and restores the executing thread's prior context afterward.
 * Wrappers must not retain an active caller-thread scope. Executor decorators share the delegate's
 * lifecycle; closing a service decorator shuts down its delegate.
 */
@Experimental
public interface TaskWrapping {

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
