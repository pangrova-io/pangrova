package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.List;
import java.util.Map;

/**
 * Carries query-level context from the dispatch entry point through the DAG engine.
 * Threads snapshot overrides, original SQL, pre-optimization column names,
 * and optional DML write configuration to stage scheduling.
 *
 * @param snapshotOverrides  time-travel snapshot overrides per table (empty if none)
 * @param originalSql        original SQL text for plan serialization retry (nullable)
 * @param preOptColumnNames  pre-optimization column names for OUTPUT stage (empty if none)
 * @param writeConfig        DML write configuration (null for read queries)
 */
public record DAGQueryContext(
        Map<String, Long> snapshotOverrides,
        String originalSql,
        List<String> preOptColumnNames,
        org.pangrova.protocol.rest.config.WriteConfig writeConfig) {

    public DAGQueryContext {
        snapshotOverrides = snapshotOverrides != null ? Map.copyOf(snapshotOverrides) : Map.of();
        preOptColumnNames = preOptColumnNames != null ? List.copyOf(preOptColumnNames) : List.of();
    }

    /** Creates a context for read-only queries with no overrides. */
    public static DAGQueryContext readOnly() {
        return new DAGQueryContext(Map.of(), null, List.of(), null);
    }
}
