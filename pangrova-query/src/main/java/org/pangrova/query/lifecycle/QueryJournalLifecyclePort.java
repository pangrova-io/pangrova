package org.pangrova.query.lifecycle;

import java.util.function.Predicate;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceAdmission;
import org.pangrova.query.api.journal.QueryAcceptanceRequest;
import org.pangrova.query.api.session.QuerySession;

import java.util.Optional;

/** Query lifecycle facts recorded into the sole configured state journal. */
public interface QueryJournalLifecyclePort extends QueryStateJournalPort {

    void queued(String queryId, String sql, QuerySession session, QueryStateMachine state, ExecutionAnchor anchor);

    QueryAcceptanceAdmission accept(String queryId, String sql, QuerySession session,
            QueryStateMachine state, QueryAcceptanceRequest request, long retentionMs, ExecutionAnchor anchor);

    Optional<QueryAcceptance> findAcceptance(String principal, String idempotencyKeyDigest);

    void purgeExpiredAcceptances();

    void terminal(QueryTerminalCleanup.Context context);

    Optional<QueryState> lookup(String queryId);

    Optional<QueryState> lookupForPrincipal(String queryId, String principal);

    void setOwner(String owner);

    Optional<Predicate<ExecutionAnchor>> durableRetirement();

    void purgeLocal(String queryId, QueryStateMachine state);

    boolean hasLatestEvent(String queryId);
}
