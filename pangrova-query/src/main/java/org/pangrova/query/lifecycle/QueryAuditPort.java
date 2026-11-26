package org.pangrova.query.lifecycle;

import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.lineage.QueryLineageFact;
import org.pangrova.query.api.session.QuerySession;

/** Audit, event and lineage effects emitted for one logical query lifecycle. */
public interface QueryAuditPort {

    QueryLineageFact prepareSubmittedLineage(String queryId, String sql, QuerySession session,
            QueryStateMachine state);

    void emitAuditSubmitted(String queryId, String sql, QuerySession session, QueryStateMachine state);

    void emitAuditCompleted(String queryId, QueryStateMachine state);

    void emitAuditFailed(String queryId, QueryStateMachine state, QueryFailure error);

    void emitAuditCancelled(String queryId, QueryStateMachine state);

    void emitTableAccessedAudit(String queryId, String catalog, String schema,
            String table, QuerySession session);

    QueryLineageFact terminalFact(String queryId, QueryStateMachine state, QueryState target);

    void persistTerminalLineage(String queryId, QueryStateMachine state);
}
