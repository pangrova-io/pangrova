package org.pangrova.persistence.query.postgresql.state;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.TaskStateEvent;

/** Owns admitted events and entered SQL operations until their actual completion. */
@ThreadSafe
final class JournalPendingWrites {
    private final int capacity;
    @GuardedBy("this")
    private final Map<Key, Write> writes = new LinkedHashMap<>();
    // Derived ordering only: immutable events and their attempts remain owned by writes.
    @GuardedBy("this")
    private final Map<String, LinkedHashSet<Key>> queryOrder = new LinkedHashMap<>();
    @GuardedBy("this")
    private final Set<String> purging = new HashSet<>();
    @GuardedBy("this")
    private final Set<JournalSqlOperation> sqlOwners = new HashSet<>();
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private boolean flushRequested;
    @GuardedBy("this")
    private int enteredSql;
    @GuardedBy("this")
    private Throwable semanticFailure;

    JournalPendingWrites(int capacity) { this.capacity = capacity; }

    CompletableFuture<Void> admit(Write candidate, boolean sync) {
        synchronized (this) {
            if (candidate.owner != this) throw rejected("Journal event belongs to a different pending owner", null);
            requireOpen();
            if (purging.contains(candidate.queryId())) throw rejected("Query journal is being purged", null);
            Write owner = writes.get(candidate.key());
            if (owner != null) return joinExisting(owner, candidate, sync);
            if (writes.size() >= capacity) throw rejected("Journal resident-event capacity exceeded", null);
            register(candidate);
            flushRequested |= sync;
            notifyAll();
            return candidate.attempt;
        }
    }

    private void register(Write candidate) {
        String queryId = candidate.queryId();
        var order = queryOrder.get(queryId);
        if (order == null) order = new LinkedHashSet<>();
        if (order.contains(candidate.key())) {
            throw new IllegalStateException("Journal ordering index contains an unowned event");
        }
        try {
            order.add(candidate.key());
            queryOrder.putIfAbsent(queryId, order);
            writes.put(candidate.key(), candidate);
        } catch (RuntimeException | Error failure) {
            // No flush can enter this same lock before the admission is complete.
            writes.remove(candidate.key(), candidate);
            order.remove(candidate.key());
            if (order.isEmpty()) queryOrder.remove(queryId, order);
            if (failure instanceof Error error) throw error;
            throw rejected("Failed to register journal admission", failure);
        }
    }

    private CompletableFuture<Void> joinExisting(Write owner, Write candidate, boolean sync) {
        if (!owner.sameEvent(candidate)) throw rejected("Journal event ID has a different pending payload", null);
        if (owner.attempt.isCompletedExceptionally()) owner.attempt = new CompletableFuture<>();
        flushRequested |= sync;
        notifyAll();
        return owner.attempt;
    }

    synchronized Batch awaitBatch(int size, long intervalMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(intervalMs);
        // Coalesce a small single-query batch, but never delay each query in an existing backlog.
        while (!flushRequested && !sealed && queryOrder.size() <= 1 && writes.size() < size) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) break;
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        if (sealed && writes.isEmpty() && enteredSql > 0) wait(intervalMs);
        if (sealed && writes.isEmpty() && enteredSql == 0) {
            if (!queryOrder.isEmpty()) throw new IllegalStateException("Journal ordering index outlived its events");
            return null;
        }
        flushRequested = false;
        return selectQueryBatch(size);
    }

    /** Called only under this owner's lock; selected events remain in the authoritative resident map. */
    private Batch selectQueryBatch(int size) {
        List<Write> batch = new ArrayList<>(Math.min(size, writes.size()));
        if (!queryOrder.isEmpty()) {
            var query = queryOrder.entrySet().iterator().next();
            for (Key key : query.getValue()) {
                Write owner = Objects.requireNonNull(writes.get(key), "Journal ordering index lost its event owner");
                if (owner.attempt.isDone()) owner.attempt = new CompletableFuture<>();
                batch.add(owner);
                if (batch.size() == size) break;
            }
            // One query per transaction, FIFO within it and round-robin between queries.
            // Work is proportional to the selected batch, never the resident capacity.
            String queryId = query.getKey();
            var order = query.getValue();
            queryOrder.remove(queryId);
            queryOrder.put(queryId, order);
        }
        return new Batch(List.copyOf(batch), batch.stream().map(write -> write.attempt).toList());
    }

    void committed(Batch batch) {
        synchronized (this) {
            for (Write owner : batch.writes()) removeOwned(owner);
            notifyAll();
        }
        batch.attempts().forEach(attempt -> attempt.complete(null));
    }

    void failed(Batch batch, Throwable error, Key confirmedRejected) {
        JournalWriteException failure = failure("Journal commit not established", error);
        synchronized (this) {
            for (Write owner : batch.writes()) {
                owner.lastFailure = error;
                if (owner.key().equals(confirmedRejected)) {
                    removeOwned(owner);
                    if (semanticFailure == null) semanticFailure = error;
                }
            }
            notifyAll();
        }
        batch.attempts().forEach(attempt -> attempt.completeExceptionally(failure));
    }

    private void removeOwned(Write owner) {
        var order = queryOrder.get(owner.queryId());
        if (writes.get(owner.key()) != owner || order == null || !order.contains(owner.key())) {
            throw new IllegalStateException("Journal event and ordering ownership diverged");
        }
        writes.remove(owner.key(), owner);
        order.remove(owner.key());
        if (order.isEmpty()) queryOrder.remove(owner.queryId(), order);
    }

    synchronized JournalSqlOperation enterSql() {
        requireUnsealed();
        JournalSqlOperation operation = new JournalSqlOperation(this, null);
        sqlOwners.add(operation);
        enteredSql++;
        return operation;
    }

    synchronized JournalSqlOperation enterPurge(String queryId) {
        requireUnsealed();
        JournalSqlOperation operation = new JournalSqlOperation(this, queryId);
        if (!purging.add(queryId)) throw failure("Query journal purge already entered", null);
        try {
            sqlOwners.add(operation);
        } catch (RuntimeException | Error failure) {
            purging.remove(queryId);
            throw failure;
        }
        enteredSql++;
        flushRequested = true;
        notifyAll();
        return operation;
    }

    synchronized void awaitQueryWrites(String queryId, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (true) {
            var order = queryOrder.get(queryId);
            if (order == null) return;
            Throwable failure = order.stream().map(writes::get).map(w -> w.lastFailure)
                    .filter(Objects::nonNull).findFirst().orElse(null);
            if (failure != null) throw failure("Query still owns uncommitted journal events", failure);
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw failure("Timed out draining query journal events", null);
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
    }

    synchronized void seal() {
        sealed = true;
        notifyAll();
    }
    synchronized void pauseRetry(long intervalMs) throws InterruptedException { wait(intervalMs); }
    synchronized boolean drained() { return writes.isEmpty() && queryOrder.isEmpty() && enteredSql == 0; }
    synchronized Throwable semanticFailure() { return semanticFailure; }
    synchronized Throwable pendingFailure() {
        Throwable failure = writes.values().stream().map(w -> w.lastFailure)
                .filter(Objects::nonNull).findFirst().orElse(null);
        return failure != null ? failure : sqlOwners.stream().map(JournalSqlOperation::failure)
                .filter(Objects::nonNull).findFirst().orElse(null);
    }

    synchronized List<JournalSqlOperation> sqlOwners() { return List.copyOf(sqlOwners); }

    synchronized void leaveSql(JournalSqlOperation operation, String purgeQuery) {
        if (!sqlOwners.remove(operation)) throw new IllegalStateException("Journal SQL owner departed twice");
        enteredSql--;
        if (purgeQuery != null) purging.remove(purgeQuery);
        notifyAll();
    }

    private void requireOpen() {
        requireUnsealed();
        if (semanticFailure != null) throw rejected("Journal rejected a conflicting immutable event", semanticFailure);
    }

    private void requireUnsealed() {
        if (sealed) throw rejected("Query journal admission is closed", null);
    }

    static JournalWriteException failure(String message, Throwable cause) {
        return new JournalWriteException(message, cause != null ? cause : new IllegalStateException(message),
                JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
    }

    private static JournalWriteException rejected(String message, Throwable cause) {
        return new JournalWriteException(message, cause != null ? cause : new IllegalStateException(message),
                JournalWriteException.AdmissionOutcome.REJECTED_BEFORE_TRANSFER);
    }

    static final class Write {
        private final JournalPendingWrites owner;
        final QueryStateEvent query;
        final TaskStateEvent task;
        private final Key key;
        @GuardedBy("owner")
        private CompletableFuture<Void> attempt = new CompletableFuture<>();
        @GuardedBy("owner")
        private Throwable lastFailure;

        Write(JournalPendingWrites owner, QueryStateEvent event) {
            this.owner = Objects.requireNonNull(owner, "owner");
            query = Objects.requireNonNull(event);
            task = null;
            key = new Key(true, event.context().eventId());
        }
        Write(JournalPendingWrites owner, TaskStateEvent event) {
            this.owner = Objects.requireNonNull(owner, "owner");
            query = null;
            task = Objects.requireNonNull(event);
            key = new Key(false, event.context().eventId());
        }
        JournalEventContext context() { return query != null ? query.context() : task.context(); }
        Key key() { return key; }
        String queryId() { return query != null ? query.queryId() : task.queryId(); }
        boolean sameEvent(Write other) {
            return Objects.equals(query, other.query) && Objects.equals(task, other.task);
        }
    }

    record Key(boolean query, UUID eventId) { }

    record Batch(List<Write> writes, List<CompletableFuture<Void>> attempts) { }

}
