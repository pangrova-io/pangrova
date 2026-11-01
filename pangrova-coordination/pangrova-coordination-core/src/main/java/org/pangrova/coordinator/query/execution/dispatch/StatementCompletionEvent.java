package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
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

    /** Resolves the typed target from the original statement and its admitted session. */
    public QualifiedTableName mutatedTableIdentity() {
        return TableNameExtractor.extractQualified(statement, session);
    }

    /** Convenience accessor for the display table name, or null. */
    public String mutatedTable() {
        return result.mutatedTable();
    }
}
