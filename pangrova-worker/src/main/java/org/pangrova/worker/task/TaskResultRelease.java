package org.pangrova.worker.task;

import org.pangrova.common.concurrency.GuardedBy;
import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Owns the result expiry and drain notification for one exact Worker task. */
@ThreadSafe
final class TaskResultRelease {
    private static final Logger LOG = LoggerFactory.getLogger(TaskResultRelease.class);

    private final TaskInfo task;
    private final WorkerTaskRegistry tasks;
    private final ScheduledExecutorService executor;
    private final Function<TaskInfo, CompletionStage<Void>> release;
    private final Runnable retryInitial;
    @GuardedBy("this")
    private ScheduledFuture<?> scheduled;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private boolean ready;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private boolean failed;

    TaskResultRelease(TaskInfo task, WorkerTaskRegistry tasks, ScheduledExecutorService executor,
                      Function<TaskInfo, CompletionStage<Void>> release, Runnable retryInitial) {
        this.task = task;
        this.tasks = tasks;
        this.executor = executor;
        this.release = release;
        this.retryInitial = retryInitial;
    }

    synchronized void expireAt(long deadlineMillis) {
        long now = System.currentTimeMillis();
        if (!closed && !ready) submit(deadlineMillis <= now ? 0 : deadlineMillis - now);
    }

    /** A Native callback may only enqueue work. Release joins Native owners on the cleanup thread. */
    synchronized void drained() {
        if (closed || ready) return;
        ready = true;
        submit(0);
    }

    void retryOrExpire(boolean expired) {
        synchronized (this) {
            if (closed || running) return;
            if (failed || (expired && !ready)) {
                ready = true;
                submit(0);
                return;
            }
        }
        retryInitial.run();
    }

    private void submit(long delayMillis) {
        try {
            ScheduledFuture<?> replacement = executor.schedule(this::run, delayMillis, TimeUnit.MILLISECONDS);
            if (scheduled != null) scheduled.cancel(false);
            scheduled = replacement;
        } catch (RejectedExecutionException failure) {
            // The timer owner may already be joining shutdown. Exact task ownership stays tracked.
            failed = true;
            LOG.warn("event=result_release_dispatch_rejected taskId={}", task.taskId(), failure);
        }
    }

    private void run() {
        synchronized (this) {
            if (closed || running || tasks.get(task.taskId()) != task) return;
            running = true;
            failed = false;
        }
        try {
            release.apply(task).whenComplete((ignored, failure) -> {
                if (failure == null) tasks.remove(task);
                else releaseFailed(failure);
            });
        } catch (RuntimeException | Error failure) {
            releaseFailed(failure);
            if (failure instanceof Error fatal) throw fatal;
        }
    }

    private void releaseFailed(Throwable failure) {
        synchronized (this) {
            running = false;
            failed = true;
        }
        LOG.warn("event=result_release_pending taskId={}", task.taskId(), failure);
    }

    synchronized void cancel() {
        closed = true;
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
    }
}
