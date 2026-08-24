package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.coordinator.query.statement.StatementClassifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.ExplainStatement;

/**
 * Classifies the type of statement being executed.
 * Used by listeners to determine which side effects to trigger,
 * and by the dispatcher to decide routing and permission checks.
 *
 * All classification logic delegates to {@link StatementClassifier}.
 */
public enum StatementCategory {
    DDL,
    DML,
    EXPLAIN,
    SHOW,
    MAINTENANCE,
    DYNAMIC_TABLE,
    INDEX,
    CLUSTERING,
    TENANT,
    STREAM,
    QUERY,
    UNKNOWN;

    /**
     * Maps a parsed statement to its category. Statements that do not match
     * any known routable category return QUERY (planner-dispatched).
     * Unsupported statements (DESCRIBE CATALOG, ALTER CATALOG, CALL) return UNKNOWN.
     */
    public static StatementCategory categorize(Statement stmt) {
        if (StatementClassifier.isDdl(stmt)) return DDL;
        if (StatementClassifier.isDml(stmt)) return DML;
        if (stmt instanceof ExplainStatement) return EXPLAIN;
        if (StatementClassifier.isShow(stmt)) return SHOW;
        if (StatementClassifier.isMaintenance(stmt)) return MAINTENANCE;
        if (StatementClassifier.isDynamicTable(stmt)) return DYNAMIC_TABLE;
        if (StatementClassifier.isUnsupported(stmt)) return UNKNOWN;
        if (StatementClassifier.isIndex(stmt)) return INDEX;
        if (StatementClassifier.isClustering(stmt)) return CLUSTERING;
        if (StatementClassifier.isTenant(stmt)) return TENANT;
        if (StatementClassifier.isStream(stmt)) return STREAM;
        return QUERY;
    }
}
