package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.parser.ast.Statement;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Event fired after a statement completes successfully.
 * Carries context needed by listeners for side effects.
 */
public record StatementCompletionEvent(
        String queryId,
        Statement statement,
        StatementResult result,
        CoordinatorSessionContext session) {

    public StatementCompletionEvent {
        checkNotNull(queryId, "queryId");
        checkNotNull(statement, "statement");
        checkNotNull(result, "result");
    }

    /** Convenience accessor for the mutated table name, or null. */
    public String mutatedTable() {
        return result.mutatedTable();
    }
}
