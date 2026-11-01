package org.pangrova.persistence.memory.query.state;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.connector.coordination.JournalWriteException;
import org.pangrova.spi.query.state.QueryStateEvent;
import org.pangrova.persistence.api.query.state.QueryStateJournal;
import org.pangrova.spi.query.state.StateJournalConfig;
import org.pangrova.spi.query.state.TaskStateEvent;

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
    @Override public synchronized void purgeQuery(String queryId) {
        requireOpen();
        Objects.requireNonNull(queryId, "queryId");
        tasks.values().removeIf(event -> event.queryId().equals(queryId));
        queries.values().removeIf(event -> event.queryId().equals(queryId));
    }
    @Override public synchronized void close() { closed = true; }
    @Override public synchronized boolean isDrained() { return closed; }

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
                JournalWriteException.AdmissionOutcome.REJECTED_BEFORE_TRANSFER);
    }
}
