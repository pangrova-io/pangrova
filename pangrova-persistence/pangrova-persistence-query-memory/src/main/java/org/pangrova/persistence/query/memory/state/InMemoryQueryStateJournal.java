package org.pangrova.persistence.query.memory.state;

import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceAdmission;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.query.api.session.JournalSessionPayload;

/** Bounded instance-owned journal. NONE retains process-local facts, never durable or HA state. */
@ThreadSafe
public final class InMemoryQueryStateJournal implements QueryStateJournal {
    private static final Set<String> TERMINAL = Set.of("FINISHED", "FAILED", "CANCELLED");
    private final int capacity;
    @org.pangrova.common.concurrent.GuardedBy("this")
    private final LinkedHashMap<UUID, QueryStateEvent> queries = new LinkedHashMap<>();
    @org.pangrova.common.concurrent.GuardedBy("this")
    private final LinkedHashMap<UUID, TaskStateEvent> tasks = new LinkedHashMap<>();
    @org.pangrova.common.concurrent.GuardedBy("this")
    private boolean closed;

    /** Uses the same finite resident-event policy as the production NONE factory. */
    public InMemoryQueryStateJournal() { this(StateJournalConfig.NONE.maxResidentEvents()); }
    /** Creates one process-local owner with an explicit event-count admission limit. */
    public InMemoryQueryStateJournal(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        this.capacity = capacity;
    }

    @Override public synchronized void appendQueryEvent(QueryStateEvent event) {
        Objects.requireNonNull(event, "event");
        requireOpen();
        UUID id = event.context().eventId();
        QueryStateEvent old = queries.get(id);
        if (old != null) {
            requireExact(old.equals(event));
            return;
        }
        requireCapacity();
        queries.put(id, event);
    }
    @Override public void appendQueryEventSync(QueryStateEvent event) { appendQueryEvent(event); }

    @Override public synchronized QueryAcceptanceAdmission acceptQuerySync(QueryStateEvent event) {
        Objects.requireNonNull(event, "event");
        if (event.acceptance() == null) throw new IllegalArgumentException("Durable acceptance requires a fact");
        requireOpen();
        QueryStateEvent existing = findAcceptanceEvent(event.acceptance().principal(),
                event.acceptance().idempotencyKeyDigest());
        if (existing != null) {
            boolean sameQuery = existing.queryId().equals(event.queryId());
            if (!sameQuery && isExpired(existing)) {
                purgeQuery(existing.queryId());
            } else {
                if (!digestEquals(existing.acceptance().requestFingerprint(),
                        event.acceptance().requestFingerprint())) {
                    throw uniqueConflict();
                }
                return new QueryAcceptanceAdmission(sameQuery, existing.acceptance());
            }
        }
        appendQueryEvent(event);
        return new QueryAcceptanceAdmission(true, event.acceptance());
    }

    @Override public synchronized Optional<QueryAcceptance> findAcceptance(String principal,
            String idempotencyKeyDigest) {
        requireOpen();
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(idempotencyKeyDigest, "idempotencyKeyDigest");
        return Optional.ofNullable(findAcceptanceEvent(principal, idempotencyKeyDigest))
                .map(QueryStateEvent::acceptance);
    }

    @Override public synchronized void appendTaskEvent(TaskStateEvent event) {
        Objects.requireNonNull(event, "event");
        requireOpen();
        UUID id = event.context().eventId();
        TaskStateEvent old = tasks.get(id);
        if (old != null) {
            requireExact(old.equals(event));
            return;
        }
        requireCapacity();
        tasks.put(id, event);
    }
    @Override public void appendTaskEventSync(TaskStateEvent event) { appendTaskEvent(event); }

    @Override public synchronized List<QueryStateEvent> replayActiveQueries() {
        requireOpen();
        Set<String> terminal = terminalQueries();
        return queries.values().stream().filter(event -> !terminal.contains(event.queryId())).toList();
    }
    @Override public synchronized List<TaskStateEvent> replayActiveTasks() {
        requireOpen();
        Set<String> active = replayActiveQueries().stream().map(QueryStateEvent::queryId)
                .collect(java.util.stream.Collectors.toSet());
        return tasks.values().stream().filter(event -> active.contains(event.queryId())).toList();
    }
    @Override public synchronized Optional<QueryStateEvent> latestQueryEvent(String queryId) {
        requireOpen();
        Objects.requireNonNull(queryId, "queryId");
        QueryStateEvent latest = null;
        for (QueryStateEvent event : queries.values()) {
            if (event.queryId().equals(queryId)) {
                latest = event;
            }
        }
        return Optional.ofNullable(latest);
    }
    @Override public synchronized Optional<String> latestQueryStateForPrincipal(String queryId, String principal) {
        requireOpen();
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(principal, "principal");
        QueryStateEvent admission = null;
        QueryStateEvent latest = null;
        for (QueryStateEvent event : queries.values()) {
            if (!queryId.equals(event.queryId())) continue;
            latest = event;
            if (admission == null && "QUEUED".equals(event.state())) admission = event;
        }
        if (admission == null || latest == null) return Optional.empty();
        var admitted = JournalSessionPayload.admittedPrincipal(queryId, admission.sessionJson());
        return admitted.filter(principal::equals).isPresent() ? Optional.of(latest.state()) : Optional.empty();
    }

    @Override public synchronized void purgeQuery(String queryId) {
        requireOpen();
        Objects.requireNonNull(queryId, "queryId");
        tasks.values().removeIf(event -> event.queryId().equals(queryId));
        QueryStateEvent acceptance = queries.values().stream()
                .filter(event -> queryId.equals(event.queryId()) && event.acceptance() != null)
                .findFirst().orElse(null);
        if (acceptance == null || isExpired(acceptance)) {
            queries.values().removeIf(event -> event.queryId().equals(queryId));
        }
    }
    @Override public synchronized void close() { closed = true; }
    @Override public synchronized boolean isDrained() { return closed; }

    @Override public synchronized void purgeExpiredAcceptances() {
        requireOpen();
        var expired = queries.values().stream()
                .filter(event -> event.acceptance() != null && isExpired(event))
                .map(QueryStateEvent::queryId).toList();
        for (String queryId : expired) {
            tasks.values().removeIf(event -> queryId.equals(event.queryId()));
            queries.values().removeIf(event -> queryId.equals(event.queryId()));
        }
    }

    private QueryStateEvent findAcceptanceEvent(String principal, String idempotencyKeyDigest) {
        for (QueryStateEvent event : queries.values()) {
            if (event.acceptance() != null && event.acceptance().principal().equals(principal)
                    && digestEquals(event.acceptance().idempotencyKeyDigest(), idempotencyKeyDigest)) {
                return event;
            }
        }
        return null;
    }

    private boolean isExpired(QueryStateEvent acceptance) {
        long terminal = queries.values().stream()
                .filter(event -> acceptance.queryId().equals(event.queryId()) && TERMINAL.contains(event.state()))
                .mapToLong(event -> event.context().timestampMs()).max().orElse(0);
        if (terminal == 0) return false;
        long retainedFrom = Math.max(acceptance.context().timestampMs(), terminal);
        long now = System.currentTimeMillis();
        return now >= retainedFrom
                && acceptance.acceptance().retentionMs() <= now - retainedFrom;
    }

    private static boolean digestEquals(String left, String right) {
        return MessageDigest.isEqual(left.getBytes(StandardCharsets.UTF_8),
                right.getBytes(StandardCharsets.UTF_8));
    }

    private static JournalWriteException uniqueConflict() {
        var conflict = new java.sql.SQLException("Idempotency key conflict", "23505");
        return new JournalWriteException("Idempotency key was already used for a different request",
                conflict, JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER);
    }

    private Set<String> terminalQueries() {
        return queries.values().stream().filter(event -> TERMINAL.contains(event.state()))
                .map(QueryStateEvent::queryId).collect(java.util.stream.Collectors.toSet());
    }
    private void requireOpen() {
        if (closed) throw failure("Process-local journal is closed");
    }
    private void requireCapacity() {
        if (queries.size() + tasks.size() >= capacity) {
            throw failure("Process-local journal resident-event capacity exceeded");
        }
    }
    private static void requireExact(boolean exact) {
        if (!exact) throw failure("Journal event ID has a different payload");
    }
    private static JournalWriteException failure(String message) {
        return new JournalWriteException(message, new IllegalStateException(message),
                JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER);
    }
}
