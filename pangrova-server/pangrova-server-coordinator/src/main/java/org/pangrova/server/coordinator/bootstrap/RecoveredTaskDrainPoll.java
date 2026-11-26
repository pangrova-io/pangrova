package org.pangrova.server.coordinator.bootstrap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.pangrova.coordinator.scheduler.SchedulerCancelResult;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.DrainReceipt;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorCallbackLifetime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collects compact Worker exit evidence independently of logical query replay. */
final class RecoveredTaskDrainPoll implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RecoveredTaskDrainPoll.class);
    private static final long CALLBACK_DRAIN_TIMEOUT_MS = 5_000;

    private final RecoveredTaskDrainJournal journal;
    private final Function<TaskStateEvent, CompletableFuture<SchedulerCancelResult>> seal;
    private final Executor executor;
    private final long retryNanos;
    private final CoordinatorCallbackLifetime callbacks = new CoordinatorCallbackLifetime();
    private final LinkedHashMap<String, Scan> candidates = new LinkedHashMap<>();
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduled;
    private long scheduledAt;
    private long generation;
    private boolean running;
    private boolean closed;

    RecoveredTaskDrainPoll(RecoveredTaskDrainJournal journal,
            Function<TaskStateEvent, CompletableFuture<SchedulerCancelResult>> seal,
            Executor executor, long retryMillis) {
        this.journal = journal;
        this.seal = seal;
        this.executor = executor;
        retryNanos = TimeUnit.MILLISECONDS.toNanos(retryMillis);
    }

    synchronized void start(ScheduledExecutorService schedulerOwner) {
        if (closed || this.scheduler != null) throw new IllegalStateException("Task drain recovery cannot start");
        this.scheduler = schedulerOwner;
        scheduleNext();
    }

    synchronized void track(List<ExecutionAnchor> claims) {
        if (closed) return;
        for (var claim : claims) {
            candidates.compute(claim.executionId(), (id, current) ->
                    current == null || claim.fencingToken() > current.claim().fencingToken()
                            ? new Scan(claim, 0, false, System.nanoTime()) : current);
        }
        scheduleNext();
    }

    private void scheduleNext() {
        if (closed || running || scheduler == null || candidates.isEmpty()) return;
        long now = System.nanoTime();
        long delay = candidates.values().stream()
                .mapToLong(candidate -> Math.max(0, candidate.readyAtNanos() - now)).min().orElseThrow();
        if (scheduled != null) {
            if (scheduledAt - now <= delay) return;
            scheduled.cancel(false);
        }
        scheduledAt = now + delay;
        long ticket = ++generation;
        try {
            scheduled = scheduler.schedule(() -> submit(ticket), delay, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException rejected) {
            scheduled = null;
            LOG.warn("event=recovered_task_drain_schedule_rejected error={}", rejected.getMessage());
        }
    }

    private void submit(long ticket) {
        Scan scan;
        CoordinatorCallbackLifetime.Operation operation;
        synchronized (this) {
            if (closed || ticket != generation) return;
            scheduled = null;
            long now = System.nanoTime();
            var ready = candidates.values().iterator();
            do {
                if (!ready.hasNext()) {
                    scheduleNext();
                    return;
                }
                scan = ready.next();
            } while (now - scan.readyAtNanos() < 0);
            operation = callbacks.enter();
            ready.remove();
            running = true;
        }
        Scan next = scan;
        try {
            executor.execute(() -> operation.run(() -> scanPage(next)));
        } catch (RejectedExecutionException rejected) {
            operation.close();
            complete(next.delayed(retryNanos));
        }
    }

    private void scanPage(Scan current) {
        Scan next = current.delayed(retryNanos);
        try {
            next = reconcile(current);
        } catch (RuntimeException failure) {
            LOG.warn("event=recovered_task_drain_scan_failed queryId={} error={}",
                    current.claim().executionId(), failure.getMessage(), failure);
        } finally {
            complete(next);
        }
    }

    private Scan reconcile(Scan current) {
        var page = journal.pendingTaskDrains(current.claim(), current.afterRowId());
        var replies = page.stream().map(attempt -> requestDrain(attempt.event())).toList();
        CompletableFuture.allOf(replies.toArray(CompletableFuture<?>[]::new)).join();
        List<DrainReceipt> receipts = new ArrayList<>(replies.size());
        for (int index = 0; index < replies.size(); index++) {
            var result = replies.get(index).join();
            var attempt = page.get(index);
            if (result.isSuccess()) {
                receipts.add(new DrainReceipt(attempt.rowId(), result.executionDrainReceipt().toByteArray()));
            } else {
                LOG.debug("event=recovered_task_drain_pending queryId={} taskId={} status={} reason={}",
                        attempt.event().queryId(), attempt.event().taskId(), result.status(), result.errorMessage());
            }
        }
        if (!receipts.isEmpty() && !journal.retainTaskDrains(current.claim(), receipts)) return null;
        boolean unresolved = current.unresolved() || receipts.size() < page.size();
        if (page.size() == RecoveredTaskDrainJournal.PAGE_SIZE) {
            return new Scan(current.claim(), page.get(page.size() - 1).rowId(), unresolved, System.nanoTime());
        }
        return unresolved ? new Scan(current.claim(), 0, false, System.nanoTime() + retryNanos) : null;
    }

    /** One unavailable attempt must not discard the receipts or callback lifetime of its peers. */
    private CompletableFuture<SchedulerCancelResult> requestDrain(TaskStateEvent event) {
        try {
            return seal.apply(event).handle((result, failure) -> {
                if (failure != null) {
                    return SchedulerCancelResult.failed(failure.toString(), true);
                }
                return result != null ? result : SchedulerCancelResult.failed("Missing drain result", true);
            });
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(SchedulerCancelResult.failed(failure.toString(), true));
        }
    }

    private synchronized void complete(Scan next) {
        if (!closed && next != null) candidates.putIfAbsent(next.claim().executionId(), next);
        running = false;
        scheduleNext();
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            candidates.clear();
            if (scheduled != null) scheduled.cancel(false);
        }
        callbacks.close(CALLBACK_DRAIN_TIMEOUT_MS);
    }

    private record Scan(ExecutionAnchor claim, long afterRowId, boolean unresolved, long readyAtNanos) {
        Scan delayed(long delayNanos) {
            return new Scan(claim, afterRowId, unresolved, System.nanoTime() + delayNanos);
        }
    }
}
