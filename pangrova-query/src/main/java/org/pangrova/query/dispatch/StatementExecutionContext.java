package org.pangrova.query.dispatch;

import org.pangrova.query.api.audit.QueryAuditAttribution;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Per-execution context passed to statement factories.
 * Carries only query-domain runtime state so that statement execution
 * contracts stay independent of planner and coordinator implementations.
 */
public record StatementExecutionContext(
        QueryStateMachine stateMachine,
        String queryId,
        QuerySession session) {

    public StatementExecutionContext {
        checkNotNull(queryId, "queryId");
    }

    /** Copies attribution without retaining state, session properties or credentials. */
    public QueryAuditAttribution auditAttribution() {
        return new QueryAuditAttribution(queryId,
                session == null || session.userIdentity() == null ? "system" : session.userIdentity().principal(),
                session == null ? null : session.tenantId());
    }
}
