package org.pangrova.observability.logging;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.pangrova.spi.observability.Context;

/**
 * MDC-based implementation of {@link Context.TaskWrapping} that captures and restores
 * SLF4J MDC context (trace_id, queryId, taskId) across async thread boundaries.
 *
 * <p>Captures MDC snapshot at decoration time (immutable copy), restores on the target
 * thread before execution, and cleans up after execution to prevent MDC leakage in
 * pooled threads.
 *
 * <p>Thread safety: stateless singleton. All state is captured per-wrap invocation
 * into the returned task closure.
 */
public final class MdcTaskWrapping implements Context.TaskWrapping {

    @Override
    public Runnable wrap(Runnable task) {
        if (task == null) {
            throw new NullPointerException("task must not be null");
        }
        Map<String, String> snapshot = LogContext.snapshot();
        return () -> {
            LogContext.restore(snapshot);
            try {
                task.run();
            } finally {
                LogContext.clear();
            }
        };
    }

    @Override
    public <T> Callable<T> wrap(Callable<T> task) {
        if (task == null) {
            throw new NullPointerException("task must not be null");
        }
        Map<String, String> snapshot = LogContext.snapshot();
        return () -> {
            LogContext.restore(snapshot);
            try {
                return task.call();
            } finally {
                LogContext.clear();
            }
        };
    }

    @Override
    public Executor wrapExecutor(Executor delegate) {
        if (delegate == null) {
            throw new NullPointerException("delegate must not be null");
        }
        return command -> delegate.execute(wrap(command));
    }

    @Override
    public ExecutorService wrapExecutorService(ExecutorService delegate) {
        if (delegate == null) {
            throw new NullPointerException("delegate must not be null");
        }
        return new MdcPropagatingExecutorService(delegate, this);
    }
}
