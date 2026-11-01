package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

import java.util.Optional;
import org.pangrova.spi.query.state.QueryStateEvent;
import org.pangrova.persistence.api.query.state.QueryStateJournal;
import org.pangrova.spi.connector.coordination.JournalWriteException;
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

    void queued(String queryId, String sql, CoordinatorSessionContext session, QueryStateMachine state) {
        QueryStateEvent fact = state.journalEvent(QueryStateMachine.State.QUEUED,
                () -> new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(
                        System.currentTimeMillis(), coordinatorId), queryId, sql,
                        org.pangrova.coordinator.query.session.JournalSessionPayload.encode(session),
                        QueryStateMachine.State.QUEUED.name(), state.lineageStart()));
        record(state, () -> journal.appendQueryEventSync(fact));
    }

    void terminal(QueryTerminalCleanup.Context context) {
        if (!context.state().hasJournalOwnership()) {
            if (audit != null) audit.persistTerminalLineage(context.queryId(), context.state());
            return;
        }
        journal.appendQueryEventSync(context.state().journalEvent(context.state().getState(),
                () -> new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(
                        System.currentTimeMillis(), coordinatorId), context.queryId(), null, null,
                        context.state().getState().name(), audit == null ? null
                                : audit.terminalFact(context.queryId(), context.state(), context.state().getState()))));
    }

    void registerListener(String queryId, QueryStateMachine state) {
        state.setTransitionRecorder((previous, current) -> {
            if (!state.hasJournalOwnership()) return;
            if (current == QueryStateMachine.State.PLANNING || current == QueryStateMachine.State.SCHEDULING
                    || current == QueryStateMachine.State.RUNNING || current == QueryStateMachine.State.FINISHING
                    || current == QueryStateMachine.State.CANCELLED) {
                journal.appendQueryEvent(state.journalEvent(current,
                        () -> new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(
                                System.currentTimeMillis(), coordinatorId), queryId, null, null, current.name(),
                                current == QueryStateMachine.State.CANCELLED && audit != null
                                        ? audit.terminalFact(queryId, state, current) : null)));
            }
        });
    }

    private static void record(QueryStateMachine state, Runnable append) {
        try {
            append.run();
            state.retainJournalOwnership();
        } catch (JournalWriteException failure) {
            if (failure.admissionOutcome() == JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED) {
                state.retainJournalOwnership();
            }
            throw failure;
        }
    }

    Optional<QueryStateMachine.State> lookup(String queryId) {
        return journal.latestQueryEvent(queryId).flatMap(event -> {
            try {
                return Optional.of(QueryStateMachine.State.valueOf(event.state()));
            } catch (IllegalArgumentException malformed) {
                LOG.warn("event=journal_state_unparseable queryId={} state={}", queryId, event.state());
                return Optional.empty();
            }
        });
    }
}
