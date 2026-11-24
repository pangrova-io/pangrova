package org.pangrova.query.lifecycle;

import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.session.QuerySession;

import java.util.Optional;

/** Query lifecycle facts recorded into the sole configured state journal. */
public interface QueryJournalLifecyclePort extends QueryStateJournalPort {

    void queued(String queryId, String sql, QuerySession session, QueryStateMachine state);

    void terminal(QueryTerminalCleanup.Context context);

    void registerListener(String queryId, QueryStateMachine state);

    Optional<QueryState> lookup(String queryId);

    Optional<QueryState> lookupForPrincipal(String queryId, String principal);

    void setOwner(String owner);

    void purge(String queryId, QueryStateMachine state);

    boolean hasLatestEvent(String queryId);
}
