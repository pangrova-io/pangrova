package org.pangrova.query.lifecycle;

import org.pangrova.query.api.enums.QueryState;

import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.pangrova.common.concurrent.GuardedBy;
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
    @GuardedBy("tickets")
    private final PriorityQueue<Ticket> tickets = new PriorityQueue<>(java.util.Comparator.comparingLong(Ticket::due));
    private final QueryExecutionCleanup executionCleanup;
    private final Consumer<Context> journal;
    private final Consumer<Context> observability;
    private final Consumer<Context> physicalRelease;
    private final LongSupplier clock;
    private volatile RetryDispatch dispatch;

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

    /** Borrows the timer and worker for cleanup retries; the caller retains shutdown ownership. */
    public void startRetryDispatch(ScheduledExecutorService timer, Executor worker) {
        dispatch = new RetryDispatch(java.util.Objects.requireNonNull(timer, "timer"),
                java.util.Objects.requireNonNull(worker, "worker"));
    }

    public void stopRetryDispatch() {
        dispatch = null;
    }

    /** Applies logical terminal effects and retains incomplete physical cleanup under its per-query owner. */
    public void complete(String queryId, QueryStateMachine state, String reason) {
        if (!state.isTerminal() || state.physicalCleanupApplied()) return;
        Completion owner = pending.computeIfAbsent(queryId,
                ignored -> new Completion(new Context(queryId, state, reason)));
        synchronized (owner) {
            if (owner.context.state() != state || !start(owner)) return;
            cancelTicket(owner, false);
        }
        apply(owner);
    }

    private boolean start(Completion owner) {
        String queryId = owner.context.queryId();
        if (owner.inProgress || pending.get(queryId) != owner) return false;
        if (owner.context.state().physicalCleanupApplied()) {
            pending.remove(queryId, owner);
            return false;
        }
        owner.inProgress = true;
        return true;
    }

    private void apply(Completion owner) {
        Context context = owner.context;
        boolean awaitingDrain = false;
        boolean succeeded = false;
        try {
            publishLogicalTerminal(context);
            awaitingDrain = !tryCleanup(context);
            succeeded = !awaitingDrain;
        } catch (RuntimeException | Error failure) {
            LOG.warn("event=query_terminal_cleanup_pending queryId={} state={}",
                    context.queryId(), context.state().getState(), failure);
            if (!context.state().terminalEffectsApplied() || failure instanceof Error) throw failure;
        } finally {
            finish(owner, succeeded, awaitingDrain);
        }
    }

    private void finish(Completion owner, boolean succeeded, boolean awaitingDrain) {
        synchronized (owner) {
            owner.inProgress = false;
            owner.awaitingDrain = awaitingDrain;
            if (succeeded) {
                pending.remove(owner.context.queryId(), owner);
            } else if (!awaitingDrain || owner.drainProgressObserved) {
                schedule(owner, awaitingDrain ? 0 : failureDelay(owner));
            }
            owner.drainProgressObserved = false;
        }
    }

    private void publishLogicalTerminal(Context context) {
        if (context.state().terminalEffectsApplied()) return;
        journal.accept(context);
        if (context.state().claimTerminalEffects()) observability.accept(context);
    }

    private boolean tryCleanup(Context context) {
        boolean admitted = context.state().awaitOwnership();
        if (admitted && context.state().getState() != QueryState.FINISHED) {
            executionCleanup.cancelDispatchedTasks(context.queryId());
        }
        boolean ready = admitted ? executionCleanup.tryCleanupQuery(context.queryId())
                : executionCleanup.tryCleanupUnadmittedQuery(context.queryId());
        if (ready) physicalRelease.accept(context);
        return ready;
    }

    /** Queues physical cleanup when an operation exits or an exact receipt is retained. */
    public void onDrainProgress(String queryId) {
        Completion completion = pending.get(queryId);
        if (completion == null) return;
        synchronized (completion) {
            if (pending.get(queryId) != completion) return;
            // Record progress that races with the check returning false. The
            // listener never performs cleanup I/O on the caller thread.
            if (completion.inProgress) {
                completion.drainProgressObserved = true;
            } else if (completion.awaitingDrain) {
                schedule(completion, 0);
            }
        }
    }

    private static long failureDelay(Completion completion) {
        long delay = Math.min(TimeUnit.MILLISECONDS.toNanos(100) << completion.failures,
                TimeUnit.SECONDS.toNanos(MAX_RETRY_DELAY_SECONDS));
        completion.failures = Math.min(completion.failures + 1, 8);
        return delay;
    }

    private void schedule(Completion completion, long delay) {
        if (completion.queuedTicket != null) return;
        Ticket ticket = new Ticket(completion, clock.getAsLong() + delay);
        completion.queuedTicket = ticket;
        synchronized (tickets) {
            tickets.add(ticket);
        }
        RetryDispatch selected = dispatch;
        if (selected == null) return;
        try {
            completion.scheduledDispatch = selected.timer().schedule(
                    () -> dispatchTicket(selected, ticket), delay, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException rejected) {
            LOG.warn("event=query_terminal_retry_timer_rejected queryId={}", completion.context.queryId(), rejected);
        }
    }

    private void dispatchTicket(RetryDispatch selected, Ticket ticket) {
        if (dispatch != selected) return;
        try {
            selected.worker().execute(() -> retryTicket(ticket, false));
        } catch (RejectedExecutionException rejected) {
            LOG.warn("event=query_terminal_retry_dispatch_rejected queryId={}",
                    ticket.completion().context.queryId(), rejected);
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
            retryTicket(ticket, true);
        }
    }

    private void retryTicket(Ticket ticket, boolean dequeued) {
        Completion completion = ticket.completion();
        synchronized (completion) {
            if (completion.queuedTicket != ticket) return;
            cancelTicket(completion, dequeued);
            if (!start(completion)) return;
        }
        try {
            apply(completion);
        } catch (RuntimeException failure) {
            LOG.warn("event=query_terminal_completion_retry_failed queryId={}", completion.context.queryId(), failure);
        }
    }

    private void cancelTicket(Completion completion, boolean dequeued) {
        Ticket ticket = completion.queuedTicket;
        if (ticket == null) return;
        completion.queuedTicket = null;
        if (!dequeued) {
            synchronized (tickets) {
                tickets.remove(ticket);
            }
        }
        if (completion.scheduledDispatch != null) {
            completion.scheduledDispatch.cancel(false);
            completion.scheduledDispatch = null;
        }
    }

    /** Borrows the authoritative query state so completion flags remain visible to its registry owner. */
    public record Context(String queryId, QueryStateMachine state, String reason) { }

    private record Ticket(Completion completion, long due) { }

    private record RetryDispatch(ScheduledExecutorService timer, Executor worker) { }

    private static final class Completion {
        private final Context context;
        @GuardedBy("this")
        private boolean inProgress;
        @GuardedBy("this")
        private boolean awaitingDrain;
        @GuardedBy("this")
        private boolean drainProgressObserved;
        @GuardedBy("this")
        private Ticket queuedTicket;
        @GuardedBy("this")
        private ScheduledFuture<?> scheduledDispatch;
        @GuardedBy("this")
        private int failures;

        private Completion(Context context) { this.context = context; }
    }
}
