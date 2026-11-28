package org.pangrova.coordinator.query.execution.phase;

import java.util.List;
import java.util.Map;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.planner.analysis.MetadataTableQuery;
import org.pangrova.planner.analysis.PlannedQuery;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.parser.parse.TimeTravelSqlClauses;
import org.pangrova.parser.parse.TimeTravelSqlClauses.Analysis;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Immutable context passed through the query execution pipeline.
 * Each phase may produce a new context with updated fields via withXxx methods.
 */
public record QueryExecutionContext(
        QueryStateMachine stateMachine,
        String sql,
        QuerySession session,
        SqlCompiler compiler,
        PlannedQuery compiledPlan,
        List<String> preOptColumnNames,
        Map<String, Long> snapshotOverrides,
        String originalSql,
        MetadataTableQuery metadataQuery,
        Analysis parsedSql) {

    public QueryExecutionContext {
        checkNotNull(stateMachine, "stateMachine");
        checkNotNull(sql, "sql");
        checkNotNull(session, "session");
        preOptColumnNames = preOptColumnNames != null ? List.copyOf(preOptColumnNames) : List.of();
        snapshotOverrides = snapshotOverrides != null ? Map.copyOf(snapshotOverrides) : Map.of();
    }

    /** Creates initial context at pipeline entry. */
    public static QueryExecutionContext initial(
            QueryStateMachine sm, String sql,
            QuerySession session, SqlCompiler compiler) {
        return initial(sm, TimeTravelSqlClauses.extract(sql), session, compiler);
    }

    /** Starts execution with the statement already parsed at the query boundary. */
    public static QueryExecutionContext initial(
            QueryStateMachine sm, Analysis parsedSql, QuerySession session, SqlCompiler compiler) {
        return new QueryExecutionContext(sm, parsedSql.sql(), session, compiler,
                null, List.of(), Map.of(), parsedSql.sql(), null, parsedSql);
    }

    /** Expands utility syntax while retaining the SQL used for audit. */
    public QueryExecutionContext withMetadataQuery(MetadataTableQuery query) {
        return new QueryExecutionContext(stateMachine, sql, session, compiler,
                compiledPlan, preOptColumnNames, snapshotOverrides, originalSql,
                checkNotNull(query, "metadataQuery"), null);
    }

    /** Passes the authenticated compilation and effective statement to dispatch. */
    public QueryExecutionContext withCompilation(SqlCompiler queryCompiler, PlannedQuery plan,
            List<String> columnNames, Analysis effectiveSql, Map<String, Long> overrides) {
        return new QueryExecutionContext(stateMachine, effectiveSql == null ? sql : effectiveSql.strippedSql(),
                session, queryCompiler, plan, columnNames, overrides, originalSql, metadataQuery, effectiveSql);
    }
}
