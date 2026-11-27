package org.pangrova.query.lifecycle;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Owns local query membership, including retained terminal states and outstanding physical cleanup. */
public final class QueryRegistry {
    private final ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
    // Initializers may call external services. Reserve the identity outside shared map operations.
    private final ConcurrentHashMap<String, QueryStateMachine> registering = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, QueryStateMachine> pendingSubmissions = new ConcurrentHashMap<>();
    private final Object submissionGate = new Object();
    private final AtomicInteger outstanding = new AtomicInteger();
    private final ReentrantLock drainLock = new ReentrantLock();
    private final Condition progressed = drainLock.newCondition();

    /** Initializes a new lifetime before making it visible. A duplicate never invokes the initializer. */
    public QueryStateMachine register(String id, QueryStateMachine state, Runnable initialize) {
        synchronized (submissionGate) {
            QueryStateMachine pending = pendingSubmissions.get(id);
            if (pending != null && pending != state) {
                throw new QueryRegistrationPendingException(id);
            }
            QueryStateMachine competing = registering.putIfAbsent(id, state);
            if (competing != null) return competing;
        }
        try {
            QueryStateMachine current = queries.get(id);
            if (current != null) return current;
            initialize.run();
            outstanding.incrementAndGet();
            queries.put(id, state);
            return null;
        } finally {
            registering.remove(id, state);
        }
    }

    /** Atomically publishes a drain-accounted candidate before admission can block. */
    public boolean reserveSubmission(String id, QueryStateMachine state) {
        synchronized (submissionGate) {
            if (queries.containsKey(id) || registering.containsKey(id)) return false;
            return pendingSubmissions.putIfAbsent(id, state) == null;
        }
    }

    /** Returns the exact candidate that has not yet entered the executable registry. */
    public QueryStateMachine pendingSubmission(String id) {
        return pendingSubmissions.get(id);
    }

    /** Releases only the exact candidate reservation after registration or definitive cleanup. */
    public void releaseSubmission(String id, QueryStateMachine state) {
        if (pendingSubmissions.remove(id, state)) signalDrained();
    }

    /** Completes local bookkeeping and publishes global drain only with committed retirement evidence. */
    public void complete(String id, QueryStateMachine state, boolean retired) {
        if (!state.claimPhysicalCleanup(retired)) return;
        if (state.discardUnadmittedQuery()) queries.remove(id, state);
        if (outstanding.decrementAndGet() == 0) {
            signalDrained();
        }
    }

    private void signalDrained() {
        if (outstanding.get() == 0 && pendingSubmissions.isEmpty()) {
            drainLock.lock();
            try {
                progressed.signalAll();
            } finally {
                drainLock.unlock();
            }
        }
    }

    public QueryStateMachine get(String id) {
        return queries.get(id);
    }

    public Map<String, QueryStateMachine> snapshot() {
        return Map.copyOf(queries);
    }

    /** Live membership for quota accounting. Clients may only observe it. */
    public Collection<QueryStateMachine> states() {
        return Collections.unmodifiableCollection(queries.values());
    }

    public void forEach(BiConsumer<String, QueryStateMachine> consumer) {
        queries.forEach(consumer);
    }

    public Set<String> outstandingIds() {
        return queries.entrySet().stream().filter(entry -> !entry.getValue().physicalCleanupApplied())
                .map(Map.Entry::getKey).collect(Collectors.toUnmodifiableSet());
    }

    public int outstandingCount() {
        return outstanding.get() + pendingSubmissions.size();
    }

    /** Admission is sealed by the caller. Pending cleanup is retried without holding the wait lock. */
    public boolean awaitDrain(long timeoutMillis, Runnable retryPending) {
        long started = System.nanoTime();
        long budget = TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeoutMillis));
        while (outstanding.get() > 0 || !pendingSubmissions.isEmpty()) {
            retryPending.run();
            drainLock.lock();
            try {
                if (outstanding.get() == 0 && pendingSubmissions.isEmpty()) return true;
                long remaining = budget - (System.nanoTime() - started);
                if (remaining <= 0) return false;
                progressed.awaitNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                drainLock.unlock();
            }
        }
        return true;
    }

    /** Removes only the observed lifetime, after both terminal effects and physical release. */
    public int evictBefore(Instant cutoff, Predicate<String> pending) {
        int removed = 0;
        for (var entry : queries.entrySet()) {
            var state = entry.getValue();
            if (state.isTerminal() && state.terminalEffectsApplied() && state.physicalCleanupApplied()
                    && state.getCreatedAt().isBefore(cutoff) && !pending.test(entry.getKey())
                    && queries.remove(entry.getKey(), state)) {
                removed++;
            }
        }
        return removed;
    }
}
