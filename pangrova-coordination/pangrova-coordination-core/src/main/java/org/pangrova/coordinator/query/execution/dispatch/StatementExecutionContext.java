package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.api.audit.QueryAuditAttribution;
import org.pangrova.query.core.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.planner.core.analysis.SqlCompiler;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Per-execution context passed to statement factories.
 * Contains runtime state needed for statement execution.
 */
public record StatementExecutionContext(
        QueryStateMachine stateMachine,
        String queryId,
        QuerySession session,
        SqlCompiler compiler,
        CoordinatorResultService resultService) {

    public StatementExecutionContext {
        checkNotNull(queryId, "queryId");
    }
    /** Copies attribution without retaining compiler, state, session properties or credentials. */
    public QueryAuditAttribution auditAttribution() {
        return new QueryAuditAttribution(queryId,
                session == null || session.userIdentity() == null ? "system" : session.userIdentity().principal(),
                session == null ? null : session.tenantId());
    }
}
