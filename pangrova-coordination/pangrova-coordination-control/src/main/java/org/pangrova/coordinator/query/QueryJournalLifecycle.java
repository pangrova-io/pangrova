package org.pangrova.coordinator.query;

import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;
import org.pangrova.query.api.exception.IdempotencyConflictException;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceAdmission;
import org.pangrova.query.api.journal.QueryAcceptanceRequest;
import org.pangrova.query.lifecycle.QueryTerminalCleanup;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.api.session.QuerySession;

import java.util.Optional;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Records query lifecycle facts in the sole configured state journal. */
final class QueryJournalLifecycle
        implements org.pangrova.query.lifecycle.QueryJournalLifecyclePort {
    private static final Logger LOG = LoggerFactory.getLogger(QueryJournalLifecycle.class);
    private final QueryStateJournal journal;
    private volatile String coordinatorId;
    private final org.pangrova.query.lifecycle.QueryAuditPort audit;

    QueryJournalLifecycle(QueryStateJournal journal) { this(journal, null); }
    QueryJournalLifecycle(QueryStateJournal journal, org.pangrova.query.lifecycle.QueryAuditPort audit) {
        this.journal = journal;
        this.audit = audit;
    }
    QueryStateJournal journal() { return journal; }
    @Override public void setOwner(String owner) { coordinatorId = owner; }
    @Override public void purge(String queryId, QueryStateMachine state) {
        if (state.hasJournalOwnership()) journal.purgeQuery(queryId);
    }

    @Override public void appendQueryEvent(QueryStateEvent event) { journal.appendQueryEvent(event); }
    @Override public void appendQueryEventSync(QueryStateEvent event) { journal.appendQueryEventSync(event); }
    @Override public void appendTaskEvent(TaskStateEvent event) { journal.appendTaskEvent(event); }
    @Override public void appendTaskEventSync(TaskStateEvent event) { journal.appendTaskEventSync(event); }
    @Override public void purgeQuery(String queryId) { journal.purgeQuery(queryId); }
    @Override public void close() { journal.close(); }
    @Override public boolean isDrained() { return journal.isDrained(); }
    @Override public java.util.List<QueryStateEvent> replayActiveQueries() {
        return journal.replayActiveQueries();
    }
    @Override public java.util.List<TaskStateEvent> replayActiveTasks() {
        return journal.replayActiveTasks();
    }
    @Override public Optional<QueryStateEvent> latestQueryEvent(String queryId) {
        return journal.latestQueryEvent(queryId);
    }
    @Override public Optional<String> latestQueryStateForPrincipal(String queryId, String principal) {
        return journal.latestQueryStateForPrincipal(queryId, principal);
    }
    @Override public boolean hasLatestEvent(String queryId) {
        return journal.latestQueryEvent(queryId).isPresent();
    }

    @Override
    public void queued(String queryId, String sql, QuerySession session, QueryStateMachine state) {
        QueryStateEvent fact = state.journalEvent(QueryState.QUEUED,
                () -> new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(
                        System.currentTimeMillis(), coordinatorId), queryId, sql,
                        org.pangrova.query.api.session.JournalSessionPayload.encode(session),
                        QueryState.QUEUED.name(), state.lineageStart()));
        record(state, () -> journal.appendQueryEventSync(fact));
    }

    @Override
    public QueryAcceptanceAdmission accept(String queryId, String sql, QuerySession session,
            QueryStateMachine state, QueryAcceptanceRequest request, long retentionMs) {
        var acceptance = QueryAcceptance.pending(request, queryId, retentionMs);
        QueryStateEvent fact = state.journalEvent(QueryState.QUEUED,
                () -> new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(
                        System.currentTimeMillis(), coordinatorId), queryId, sql,
                        org.pangrova.query.api.session.JournalSessionPayload.encode(session),
                        QueryState.QUEUED.name(), state.lineageStart(), acceptance));
        try {
            var admission = journal.acceptQuerySync(fact);
            if (admission.newExecution()) state.retainJournalOwnership();
            return admission;
        } catch (JournalWriteException failure) {
            if (failure.admissionOutcome() == JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED) {
                Optional<QueryAcceptance> existing;
                try {
                    existing = journal.findAcceptance(request.principal(), request.idempotencyKeyDigest());
                } catch (RuntimeException lookupFailure) {
                    failure.addSuppressed(lookupFailure);
                    state.retainJournalOwnership();
                    throw failure;
                }
                if (existing.isPresent()) {
                    if (!existing.get().requestFingerprint().equals(request.requestFingerprint())) {
                        throw new IdempotencyConflictException();
                    }
                    boolean newExecution = queryId.equals(existing.get().receipt().queryId());
                    if (newExecution) state.retainJournalOwnership();
                    return new QueryAcceptanceAdmission(newExecution, existing.get());
                }
                state.retainJournalOwnership();
            }
            if (isUniqueConflict(failure)) throw new IdempotencyConflictException();
            throw failure;
        }
    }

    @Override
    public Optional<QueryAcceptance> findAcceptance(String principal, String idempotencyKeyDigest) {
        return journal.findAcceptance(principal, idempotencyKeyDigest);
    }

    @Override
    public void purgeExpiredAcceptances() {
        journal.purgeExpiredAcceptances();
    }

    private static boolean isUniqueConflict(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof java.sql.SQLException sql && "23505".equals(sql.getSQLState())) return true;
            current = current.getCause();
        }
        return false;
    }

    @Override
    public void terminal(QueryTerminalCleanup.Context context) {
        if (!context.state().hasJournalOwnership()) {
            if (audit != null) audit.persistTerminalLineage(context.queryId(), context.state());
            return;
        }
        journal.appendQueryEventSync(context.state().journalEvent(context.state().getState(),
                () -> new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(
                        System.currentTimeMillis(), coordinatorId), context.queryId(), null, null,
                        context.state().getState().name(), audit == null ? null
                                : audit.terminalFact(context.queryId(), context.state(), context.state().getState()))));
    }

    @Override
    public void registerListener(String queryId, QueryStateMachine state) {
        state.setTransitionRecorder((previous, current) -> {
            if (!state.hasJournalOwnership()) return;
            if (current == QueryState.PLANNING || current == QueryState.SCHEDULING
                    || current == QueryState.RUNNING || current == QueryState.FINISHING
                    || current == QueryState.CANCELLED) {
                journal.appendQueryEvent(state.journalEvent(current,
                        () -> new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(
                                System.currentTimeMillis(), coordinatorId), queryId, null, null, current.name(),
                                current == QueryState.CANCELLED && audit != null
                                        ? audit.terminalFact(queryId, state, current) : null)));
            }
        });
    }

    private static void record(QueryStateMachine state, Runnable append) {
        try {
            append.run();
            state.retainJournalOwnership();
        } catch (JournalWriteException failure) {
            if (failure.admissionOutcome() == JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED) {
                state.retainJournalOwnership();
            }
            throw failure;
        }
    }

    @Override
    public Optional<QueryState> lookup(String queryId) {
        return journal.latestQueryEvent(queryId).flatMap(event -> readState(queryId, event.state()));
    }

    @Override
    public Optional<QueryState> lookupForPrincipal(String queryId, String principal) {
        return latestQueryStateForPrincipal(queryId, principal).flatMap(state -> readState(queryId, state));
    }

    private static Optional<QueryState> readState(String queryId, String state) {
        try {
            return Optional.of(QueryState.valueOf(state));
        } catch (IllegalArgumentException malformed) {
            LOG.warn("event=journal_state_unparseable queryId={} state={}", queryId, state);
            return Optional.empty();
        }
    }
}
