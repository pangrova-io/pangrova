package org.pangrova.observability.core.logging;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.pangrova.observability.spi.context.TaskWrapping;

/**
 * An {@link ExecutorService} wrapper that propagates MDC context for every submitted task.
 *
 * <p>Delegates all operations to the underlying service, wrapping each task with MDC
 * snapshot/restore via {@link TaskWrapping}. This ensures trace_id, queryId,
 * and taskId are available in async execution threads.
 *
 * <p>Thread safety: delegates to the underlying ExecutorService which manages its own
 * concurrency. This wrapper is stateless beyond holding references to the delegate
 * and the task wrapping instance.
 */
final class MdcPropagatingExecutorService implements ExecutorService {

    private final ExecutorService delegate;
    private final TaskWrapping taskWrapping;

    MdcPropagatingExecutorService(ExecutorService delegate, TaskWrapping taskWrapping) {
        this.delegate = delegate;
        this.taskWrapping = taskWrapping;
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(taskWrapping.wrap(command));
    }

    @Override
    public Future<?> submit(Runnable task) {
        return delegate.submit(taskWrapping.wrap(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return delegate.submit(taskWrapping.wrap(task), result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return delegate.submit(taskWrapping.wrap(task));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return delegate.invokeAll(wrapAll(tasks));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                         long timeout, TimeUnit unit)
            throws InterruptedException {
        return delegate.invokeAll(wrapAll(tasks), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        return delegate.invokeAny(wrapAll(tasks));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
                           long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(wrapAll(tasks), timeout, unit);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    private <T> List<Callable<T>> wrapAll(Collection<? extends Callable<T>> tasks) {
        return tasks.stream()
                .map(taskWrapping::wrap)
                .toList();
    }
}
