package org.pangrova.query.lifecycle;

import org.pangrova.query.api.enums.QueryState;

import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.query.api.lifecycle.QueryExecutionCleanup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retries physical cleanup independently from observable logical query termination. */
@ThreadSafe
@org.pangrova.common.api.annotation.Internal
public final class QueryTerminalCleanup {
    private static final Logger LOG = LoggerFactory.getLogger(QueryTerminalCleanup.class);
    private static final int MAX_RETRIES_PER_TICK = 4;
    private static final int MAX_RETRY_DELAY_SECONDS = 30;
    private final ConcurrentHashMap<String, Completion> pending = new ConcurrentHashMap<>();
    @org.pangrova.common.concurrent.GuardedBy("tickets")
    private final PriorityQueue<Ticket> tickets = new PriorityQueue<>(java.util.Comparator.comparingLong(Ticket::due));
    private final QueryExecutionCleanup executionCleanup;
    private final Consumer<Context> journal;
    private final Consumer<Context> observability;
    private final Consumer<Context> physicalRelease;
    private final LongSupplier clock;
    private volatile ScheduledExecutorService retryTimer;
    private volatile Executor retryWorker;

    /**
     * Borrows cleanup callbacks and a monotonic nanosecond clock for retry deadlines.
     * Callbacks must support concurrent queries; this owner serializes work per query.
     */
    public QueryTerminalCleanup(QueryExecutionCleanup executionCleanup, Consumer<Context> journal,
                         Consumer<Context> observability, Consumer<Context> physicalRelease, LongSupplier clock) {
        this.executionCleanup = java.util.Objects.requireNonNull(executionCleanup, "executionCleanup");
        this.journal = journal;
        this.observability = observability;
        this.physicalRelease = physicalRelease;
        this.clock = clock;
    }

    public boolean isPending(String queryId) { return pending.containsKey(queryId); }

    /** Borrows the timer and worker for delayed cleanup retries; the caller retains shutdown ownership. */
    public void startRetryDispatch(ScheduledExecutorService timer, Executor worker) {
        retryTimer = java.util.Objects.requireNonNull(timer, "timer");
        retryWorker = java.util.Objects.requireNonNull(worker, "worker");
    }

    public void stopRetryDispatch() {
        retryTimer = null;
        retryWorker = null;
    }

    /** Applies logical terminal effects and retains incomplete physical cleanup under its per-query owner. */
    public void complete(String queryId, QueryStateMachine state, String reason) {
        if (!state.isTerminal() || state.physicalCleanupApplied()) return;
        Completion owner = pending.computeIfAbsent(queryId,
                ignored -> state.physicalCleanupApplied() ? null : new Completion(new Context(queryId, state, reason)));
        if (owner != null) attempt(owner);
    }

    private void attempt(Completion owner) {
        if (!owner.inProgress.compareAndSet(false, true)) return;
        boolean retryNeeded = false;
        String queryId = owner.context.queryId();
        QueryStateMachine state = owner.context.state();
        try {
            if (pending.get(queryId) != owner) return;
            publishLogicalTerminal(owner.context);
            cleanup(owner.context);
            pending.remove(queryId, owner);
        } catch (RuntimeException | Error failure) {
            retryNeeded = true;
            LOG.warn("event=query_terminal_cleanup_pending queryId={} state={}", queryId, state.getState(), failure);
            if (!state.terminalEffectsApplied() || failure instanceof Error) throw failure;
        } finally {
            owner.inProgress.set(false);
            // Publish after yielding so a dispatcher cannot drop this retry against the failed attempt.
            if (retryNeeded) schedule(owner);
        }
    }

    private void publishLogicalTerminal(Context context) {
        if (context.state().terminalEffectsApplied()) return;
        journal.accept(context);
        if (context.state().claimTerminalEffects()) observability.accept(context);
    }

    private void cleanup(Context context) {
        if (!context.state().awaitOwnership()) {
            executionCleanup.cleanupUnadmittedQuery(context.queryId());
            physicalRelease.accept(context);
            return;
        }
        if (context.state().getState() != QueryState.FINISHED) {
            executionCleanup.cancelDispatchedTasks(context.queryId());
        }
        executionCleanup.cleanupQuery(context.queryId());
        physicalRelease.accept(context);
    }

    public void onExecutionDrain(String queryId) {
        retryAfterDrain(queryId);
    }

    public void onOperationExit(String queryId) {
        retryAfterDrain(queryId);
    }

    private void retryAfterDrain(String queryId) {
        Completion completion = pending.get(queryId);
        if (completion == null) return;
        // Drain listeners run on the task/frontend exit path. They must never perform
        // journal or physical cleanup I/O: a blocked database row would hold that
        // exit path (and can prevent the caller from releasing the row lock).
        schedule(completion);
    }

    private void schedule(Completion completion) {
        synchronized (completion) {
            if (completion.queuedTicket != null || pending.get(completion.context.queryId()) != completion) return;
            long delay = Math.min(TimeUnit.MILLISECONDS.toNanos(100) << completion.failures,
                    TimeUnit.SECONDS.toNanos(MAX_RETRY_DELAY_SECONDS));
            completion.failures = Math.min(completion.failures + 1, 8);
            Ticket ticket = new Ticket(completion, clock.getAsLong() + delay);
            completion.queuedTicket = ticket;
            synchronized (tickets) {
                tickets.add(ticket);
            }
            ScheduledExecutorService timer = retryTimer;
            Executor worker = retryWorker;
            if (timer != null && worker != null) {
                try {
                    timer.schedule(() -> {
                        if (retryTimer != timer || retryWorker != worker) return;
                        try {
                            worker.execute(() -> retryTicket(ticket));
                        } catch (RejectedExecutionException rejected) {
                            LOG.warn("event=query_terminal_retry_dispatch_rejected queryId={}",
                                    completion.context.queryId(), rejected);
                        }
                    }, delay, TimeUnit.NANOSECONDS);
                } catch (RejectedExecutionException rejected) {
                    LOG.warn("event=query_terminal_retry_timer_rejected queryId={}",
                            completion.context.queryId(), rejected);
                }
            }
        }
    }

    /** Attempts a bounded number of due cleanup tickets; later tickets remain queued. */
    public void retryPending() {
        for (int attempted = 0; attempted < MAX_RETRIES_PER_TICK;) {
            Ticket ticket;
            synchronized (tickets) {
                ticket = tickets.peek();
                if (ticket == null || ticket.due() - clock.getAsLong() > 0) return;
                tickets.remove();
            }
            attempted++;
            retryTicket(ticket);
        }
    }

    private void retryTicket(Ticket ticket) {
        Completion completion = ticket.completion();
        synchronized (completion) {
            if (completion.queuedTicket != ticket) return;
            completion.queuedTicket = null;
        }
        synchronized (tickets) {
            tickets.remove(ticket);
        }
        try {
            attempt(completion);
        } catch (RuntimeException failure) {
            LOG.warn("event=query_terminal_completion_retry_failed queryId={}", completion.context.queryId(), failure);
        }
    }

    /** Borrows the authoritative query state so completion flags remain visible to its registry owner. */
    public record Context(String queryId, QueryStateMachine state, String reason) { }

    private record Ticket(Completion completion, long due) { }

    private static final class Completion {
        private final Context context;
        private final AtomicBoolean inProgress = new AtomicBoolean();
        @org.pangrova.common.concurrent.GuardedBy("this")
        private Ticket queuedTicket;
        @org.pangrova.common.concurrent.GuardedBy("this")
        private int failures;

        private Completion(Context context) { this.context = context; }
    }
}
