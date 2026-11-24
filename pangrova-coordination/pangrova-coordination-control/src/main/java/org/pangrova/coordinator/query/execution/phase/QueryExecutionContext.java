package org.pangrova.coordinator.query.execution.phase;

import java.util.List;
import java.util.Map;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.planner.analysis.SqlCompiler;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable context passed through the query execution pipeline.
 * Each phase may produce a new context with updated fields via withXxx methods.
 */
public record QueryExecutionContext(
        QueryStateMachine stateMachine,
        String sql,
        QuerySession session,
        SqlCompiler compiler,
        Object compiledPlan,
        List<String> preOptColumnNames,
        String cacheKey,
        Map<String, Long> snapshotOverrides,
        String originalSql,
        org.pangrova.planner.analysis.MetadataTableQuery metadataQuery) {

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
        return new QueryExecutionContext(sm, sql, session, compiler,
                null, List.of(), null, Map.of(), sql, null);
    }

    /** Returns a copy with the compiled plan and pre-optimization column names set. */
    public QueryExecutionContext withCompiler(SqlCompiler queryCompiler) {
        return new QueryExecutionContext(stateMachine, sql, session, queryCompiler,
                compiledPlan, preOptColumnNames, cacheKey, snapshotOverrides, originalSql, metadataQuery);
    }

    /** Returns a copy with the compiled plan and pre-optimization column names set. */
    public QueryExecutionContext withCompiledPlan(Object plan, List<String> columnNames) {
        return new QueryExecutionContext(stateMachine, sql, session, compiler,
                plan, columnNames, cacheKey, snapshotOverrides, originalSql, metadataQuery);
    }

    /** Returns a copy with the compiled plan set (preserves existing pre-opt column names). */
    public QueryExecutionContext withCompiledPlan(Object plan) {
        return withCompiledPlan(plan, preOptColumnNames);
    }

    /** Expands utility syntax without changing names, literal values or audit SQL. */
    public QueryExecutionContext withMetadataQuery(org.pangrova.planner.analysis.MetadataTableQuery query) {
        return new QueryExecutionContext(stateMachine, sql, session, compiler,
                compiledPlan, preOptColumnNames, cacheKey, snapshotOverrides,
                originalSql, checkNotNull(query, "metadataQuery"));
    }

    /** Returns a copy with snapshot overrides and stripped SQL set. */
    public QueryExecutionContext withTimeTravelResult(String strippedSql, Map<String, Long> overrides) {
        return new QueryExecutionContext(stateMachine, strippedSql, session, compiler,
                compiledPlan, preOptColumnNames, cacheKey, overrides, originalSql, metadataQuery);
    }

    /** Returns a copy with the cache key set. */
    public QueryExecutionContext withCacheKey(String newCacheKey) {
        return new QueryExecutionContext(stateMachine, sql, session, compiler,
                compiledPlan, preOptColumnNames, newCacheKey, snapshotOverrides, originalSql, metadataQuery);
    }

    /** Returns a copy with the stripped SQL and cache key set. */
    public QueryExecutionContext withStrippedSql(String strippedSql, String newCacheKey) {
        return new QueryExecutionContext(stateMachine, strippedSql, session, compiler,
                compiledPlan, preOptColumnNames, newCacheKey, snapshotOverrides, originalSql, metadataQuery);
    }
}
