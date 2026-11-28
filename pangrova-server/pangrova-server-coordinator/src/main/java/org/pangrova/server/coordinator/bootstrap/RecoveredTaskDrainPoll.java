package org.pangrova.server.coordinator.bootstrap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.IssuedAttempt;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorCallbackLifetime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collects compact Worker exit evidence independently of logical query replay. */
final class RecoveredTaskDrainPoll implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RecoveredTaskDrainPoll.class);
    private static final long CALLBACK_DRAIN_TIMEOUT_MS = 5_000;
    // Bound network fan-out independently of the shared cleanup executor.
    static final int MAX_IN_FLIGHT_PAGES = 4;

    private final RecoveredTaskDrainJournal journal;
    private final Function<TaskStateEvent, CompletableFuture<SchedulerCancelResult>> seal;
    private final java.util.function.BiPredicate<ExecutionAnchor, RecoveredTaskDrainJournal.DrainOutcome> retire;
    private final Executor executor;
    private final long retryNanos;
    private final CoordinatorCallbackLifetime callbacks = new CoordinatorCallbackLifetime();
    private final LinkedHashMap<String, Scan> candidates = new LinkedHashMap<>();
    private final Map<String, Scan> inFlight = new HashMap<>();
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduled;
    private long scheduledAt;
    private long generation;
    private boolean closed;

    RecoveredTaskDrainPoll(RecoveredTaskDrainJournal journal,
            Function<TaskStateEvent, CompletableFuture<SchedulerCancelResult>> seal,
            java.util.function.BiPredicate<ExecutionAnchor, RecoveredTaskDrainJournal.DrainOutcome> retire,
            Executor executor, long retryMillis) {
        this.journal = journal;
        this.seal = seal;
        this.retire = retire;
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
            String executionId = claim.executionId();
            if (!supersedes(claim, inFlight.get(executionId))) continue;
            candidates.compute(executionId, (id, current) -> supersedes(claim, current)
                    ? new Scan(claim, 0, false, System.nanoTime()) : current);
        }
        scheduleNext();
    }

    private static boolean supersedes(ExecutionAnchor claim, Scan previous) {
        return previous == null || claim.fencingToken() > previous.claim().fencingToken();
    }

    private void scheduleNext() {
        if (closed || scheduler == null || inFlight.size() >= MAX_IN_FLIGHT_PAGES) return;
        long now = System.nanoTime();
        long delay = candidates.values().stream()
                .filter(candidate -> !inFlight.containsKey(candidate.claim().executionId()))
                .mapToLong(candidate -> Math.max(0, candidate.readyAtNanos() - now))
                .min().orElse(-1);
        if (delay < 0) return;
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
            } while (inFlight.containsKey(scan.claim().executionId()) || now - scan.readyAtNanos() < 0);
            operation = callbacks.enter();
            ready.remove();
            inFlight.put(scan.claim().executionId(), scan);
            scheduleNext();
        }
        Scan next = scan;
        try {
            executor.execute(() -> operation.run(() -> startPage(next)));
        } catch (RejectedExecutionException rejected) {
            operation.close();
            complete(next, next.delayed(retryNanos));
        }
    }

    private void startPage(Scan current) {
        boolean submitted = false;
        CoordinatorCallbackLifetime.Operation continuation = null;
        try {
            var page = journal.pendingTaskDrains(current.claim(), current.afterRowId());
            if (page.isEmpty()) {
                submitted = true;
                finishPage(current, page, List.of());
                return;
            }
            synchronized (this) {
                if (closed) return;
                continuation = callbacks.enter();
            }
            var replies = page.stream().map(attempt -> requestDrain(attempt.event())).toList();
            var responseOwner = continuation;
            CompletableFuture.allOf(replies.toArray(CompletableFuture<?>[]::new))
                    .whenComplete((ignored, failure) -> resumePage(current, page, replies, responseOwner));
            submitted = true;
        } catch (RuntimeException failure) {
            logScanFailure(current, failure);
        } finally {
            if (!submitted) {
                if (continuation != null) continuation.close();
                complete(current, current.delayed(retryNanos));
            }
        }
    }

    private void resumePage(Scan current, List<IssuedAttempt> page,
            List<CompletableFuture<SchedulerCancelResult>> replies, CoordinatorCallbackLifetime.Operation operation) {
        try {
            executor.execute(() -> operation.run(() -> finishPage(current, page, replies)));
        } catch (RejectedExecutionException rejected) {
            operation.close();
            complete(current, current.delayed(retryNanos));
        }
    }

    private void finishPage(Scan current, List<IssuedAttempt> page,
            List<CompletableFuture<SchedulerCancelResult>> replies) {
        Scan next = current.delayed(retryNanos);
        try {
            next = reconcile(current, page, replies);
        } catch (RuntimeException failure) {
            logScanFailure(current, failure);
        } finally {
            complete(current, next);
        }
    }

    private static void logScanFailure(Scan current, RuntimeException failure) {
        LOG.warn("event=recovered_task_drain_scan_failed queryId={} error={}",
                current.claim().executionId(), failure.getMessage(), failure);
    }

    private Scan reconcile(Scan current, List<IssuedAttempt> page,
            List<CompletableFuture<SchedulerCancelResult>> replies) {
        List<DrainReceipt> receipts = new ArrayList<>(replies.size());
        for (int index = 0; index < replies.size(); index++) {
            // The aggregate continuation starts only after every reply has settled.
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
        if (unresolved) return retrySweep(current);
        var outcome = journal.confirmRecoveredTaskDrains(current.claim());
        return switch (outcome) {
            case PENDING_RECEIPTS -> retrySweep(current);
            case CONFIRMED, CONFIRMED_STAGING -> retire.test(current.claim(), outcome) ? null : retrySweep(current);
            case MISSING_ISSUANCE -> {
                LOG.warn("event=recovered_task_drain_incomplete_issuance queryId={}", current.claim().executionId());
                yield null;
            }
            case PENDING_COORDINATOR_STATEMENT -> {
                LOG.info("event=recovered_statement_exit_unproven queryId={}", current.claim().executionId());
                yield null;
            }
            case LOST_CLAIM, NO_SQL_EVIDENCE, PENDING_WRITE -> null;
        };
    }

    private Scan retrySweep(Scan current) {
        return new Scan(current.claim(), 0, false, System.nanoTime() + retryNanos);
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

    private synchronized void complete(Scan current, Scan next) {
        String executionId = current.claim().executionId();
        if (!inFlight.remove(executionId, current)) return;
        if (!closed && next != null) candidates.putIfAbsent(executionId, next);
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
