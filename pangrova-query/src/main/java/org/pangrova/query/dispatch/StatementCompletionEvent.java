package org.pangrova.query.dispatch;

import org.pangrova.query.statement.StatementTableTargets;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.parser.api.ast.Statement;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Event fired after a statement completes successfully.
 * Carries context needed by listeners for side effects.
 */
public record StatementCompletionEvent(
        String queryId,
        Statement statement,
        StatementResult result,
        QuerySession session) {

    public StatementCompletionEvent {
        checkNotNull(queryId, "queryId");
        checkNotNull(statement, "statement");
        checkNotNull(result, "result");
    }

    /** Resolves the typed target from the original statement and its admitted session. */
    public QualifiedTableName mutatedTableIdentity() {
        return StatementTableTargets.resolve(statement, session);
    }

    /** Convenience accessor for the display table name, or null. */
    public String mutatedTable() {
        return result.mutatedTable();
    }
}
