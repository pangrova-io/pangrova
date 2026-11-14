package org.pangrova.coordinator.query;

import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;
import org.pangrova.query.lifecycle.QueryTerminalCleanup;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.api.session.QuerySession;

import java.util.Optional;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Records query lifecycle facts in the sole configured state journal. */
final class QueryJournalLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(QueryJournalLifecycle.class);
    private final QueryStateJournal journal;
    private volatile String coordinatorId;
    private final QueryAuditDispatcher audit;

    QueryJournalLifecycle(QueryStateJournal journal) { this(journal, null); }
    QueryJournalLifecycle(QueryStateJournal journal, QueryAuditDispatcher audit) {
        this.journal = journal;
        this.audit = audit;
    }
    QueryStateJournal journal() { return journal; }
    void setOwner(String owner) { coordinatorId = owner; }
    void purge(String queryId, QueryStateMachine state) {
        if (state.hasJournalOwnership()) journal.purgeQuery(queryId);
    }

    void queued(String queryId, String sql, QuerySession session, QueryStateMachine state) {
        QueryStateEvent fact = state.journalEvent(QueryState.QUEUED,
                () -> new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(
                        System.currentTimeMillis(), coordinatorId), queryId, sql,
                        org.pangrova.query.session.JournalSessionPayload.encode(session),
                        QueryState.QUEUED.name(), state.lineageStart()));
        record(state, () -> journal.appendQueryEventSync(fact));
    }

    void terminal(QueryTerminalCleanup.Context context) {
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

    void registerListener(String queryId, QueryStateMachine state) {
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

    Optional<QueryState> lookup(String queryId) {
        return journal.latestQueryEvent(queryId).flatMap(event -> {
            try {
                return Optional.of(QueryState.valueOf(event.state()));
            } catch (IllegalArgumentException malformed) {
                LOG.warn("event=journal_state_unparseable queryId={} state={}", queryId, event.state());
                return Optional.empty();
            }
        });
    }
}
