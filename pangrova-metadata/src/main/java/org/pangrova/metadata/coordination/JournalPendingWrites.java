package org.pangrova.metadata.coordination;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.coordination.JournalEventContext;
import org.pangrova.spi.coordination.JournalWriteException;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.TaskStateEvent;

/** Owns admitted events and entered SQL operations until their actual completion. */
@ThreadSafe
final class JournalPendingWrites {
    private final int capacity;
    @GuardedBy("this") private final Map<Key, Write> writes = new LinkedHashMap<>();
    @GuardedBy("this") private final Set<String> purging = new HashSet<>();
    @GuardedBy("this") private final Set<JournalSqlOperation> sqlOwners = new HashSet<>();
    @GuardedBy("this") private boolean sealed;
    @GuardedBy("this") private boolean flushRequested;
    @GuardedBy("this") private int enteredSql;
    @GuardedBy("this") private Throwable semanticFailure;

    JournalPendingWrites(int capacity) { this.capacity = capacity; }

    CompletableFuture<Void> admit(Write candidate, boolean sync) {
        synchronized (this) {
            requireOpen();
            if (purging.contains(candidate.queryId())) throw rejected("Query journal is being purged", null);
            Write owner = writes.get(candidate.key());
            if (owner != null) return joinExisting(owner, candidate, sync);
            if (writes.size() >= capacity) throw rejected("Journal resident-event capacity exceeded", null);
            writes.put(candidate.key(), candidate);
            flushRequested |= sync;
            notifyAll();
            return candidate.attempt;
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
        while (!flushRequested && !sealed) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) break;
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        if (sealed && writes.isEmpty() && enteredSql > 0) wait(intervalMs);
        if (sealed && writes.isEmpty() && enteredSql == 0) return null;
        flushRequested = false;
        List<Write> batch = new ArrayList<>(Math.min(size, writes.size()));
        for (Write owner : writes.values()) {
            if (owner.attempt.isDone()) owner.attempt = new CompletableFuture<>();
            batch.add(owner);
            if (batch.size() == size) break;
        }
        return new Batch(List.copyOf(batch), batch.stream().map(write -> write.attempt).toList());
    }

    void committed(Batch batch) {
        synchronized (this) {
            for (Write owner : batch.writes()) writes.remove(owner.key(), owner);
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
                    writes.remove(confirmedRejected, owner);
                    if (semanticFailure == null) semanticFailure = error;
                }
            }
            notifyAll();
        }
        batch.attempts().forEach(attempt -> attempt.completeExceptionally(failure));
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
        try { sqlOwners.add(operation); }
        catch (RuntimeException | Error failure) { purging.remove(queryId); throw failure; }
        enteredSql++;
        flushRequested = true;
        notifyAll();
        return operation;
    }

    synchronized void awaitQueryWrites(String queryId, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (true) {
            List<Write> pending = writes.values().stream().filter(w -> w.queryId().equals(queryId)).toList();
            if (pending.isEmpty()) return;
            Throwable failure = pending.stream().map(w -> w.lastFailure).filter(Objects::nonNull).findFirst().orElse(null);
            if (failure != null) throw failure("Query still owns uncommitted journal events", failure);
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw failure("Timed out draining query journal events", null);
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
    }

    synchronized void seal() { sealed = true; notifyAll(); }
    synchronized void pauseRetry(long intervalMs) throws InterruptedException { wait(intervalMs); }
    synchronized boolean drained() { return writes.isEmpty() && enteredSql == 0; }
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

    final class Write {
        final QueryStateEvent query;
        final TaskStateEvent task;
        private final Key key;
        @GuardedBy("JournalPendingWrites.this") private CompletableFuture<Void> attempt = new CompletableFuture<>();
        @GuardedBy("JournalPendingWrites.this") private Throwable lastFailure;
        Write(QueryStateEvent event) {
            query = Objects.requireNonNull(event); task = null; key = new Key(true, event.context().eventId());
        }
        Write(TaskStateEvent event) {
            query = null; task = Objects.requireNonNull(event); key = new Key(false, event.context().eventId());
        }
        JournalEventContext context() { return query != null ? query.context() : task.context(); }
        Key key() { return key; }
        String queryId() { return query != null ? query.queryId() : task.queryId(); }
        boolean sameEvent(Write other) { return Objects.equals(query, other.query) && Objects.equals(task, other.task); }
    }

    record Key(boolean query, UUID eventId) { }
    record Batch(List<Write> writes, List<CompletableFuture<Void>> attempts) { }

}
