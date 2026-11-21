package org.pangrova.execution.stage;

import java.util.List;
import java.util.Map;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.execution.stage.binding.RerankIntent;
import org.pangrova.execution.stage.binding.RetrievalGenerationIntent;
import org.pangrova.execution.stage.binding.RetrievalMergeIntent;

/**
 * Carries query-level context from the dispatch entry point through the DAG engine.
 * Threads snapshot overrides, original SQL, pre-optimization column names,
 * and optional DML write configuration to stage scheduling.
 *
 * @param snapshotOverrides  time-travel snapshot overrides per table (empty if none)
 * @param execution         live query identity or current PG-resolved durable authority
 * @param originalSql        original SQL text for plan serialization retry (nullable)
 * @param preOptColumnNames  pre-optimization column names for OUTPUT stage (empty if none)
 * @param writeConfig        DML write configuration (null for read queries)
 * @param retrievalArtifacts immutable artifact generations selected for this query;
 *                           physical objects are resolved after task partitioning
 */
public record QueryExecutionScope(
        StageExecutionContext execution,
        Map<String, Long> snapshotOverrides,
        String originalSql,
        List<String> preOptColumnNames,
        org.pangrova.protocol.rest.config.WriteConfig writeConfig,
        List<RetrievalGenerationIntent> retrievalArtifacts,
        java.util.Optional<RetrievalMergeIntent> retrievalMerge,
        java.util.Optional<RerankIntent> rerank) {

    public QueryExecutionScope {
        if (execution == null || execution.tenantId() == null || execution.tenantId().isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        snapshotOverrides = snapshotOverrides != null ? Map.copyOf(snapshotOverrides) : Map.of();
        preOptColumnNames = preOptColumnNames != null ? List.copyOf(preOptColumnNames) : List.of();
        retrievalArtifacts = retrievalArtifacts != null
                ? List.copyOf(retrievalArtifacts) : List.of();
        retrievalMerge = retrievalMerge != null
                ? retrievalMerge : java.util.Optional.empty();
        rerank = rerank != null ? rerank : java.util.Optional.empty();
        if (!(execution instanceof StageExecutionContext.Live)
                && (originalSql != null || writeConfig != null || !retrievalArtifacts.isEmpty()
                        || retrievalMerge.isPresent() || rerank.isPresent())) {
            throw new IllegalArgumentException("Durable DAG consumes its immutable envelope, not live binding or SQL");
        }
    }

    /** Creates a context for read-only queries with no overrides. */
    public static QueryExecutionScope readOnly(QuerySession session) {
        return new QueryExecutionScope(
                new StageExecutionContext.Live(session), Map.of(), null, List.of(), null, List.of(),
                java.util.Optional.empty(), java.util.Optional.empty());
    }

    public String tenantId() {
        return execution.tenantId();
    }

    /** Requires the actual live identity before catalog lookup or model discovery. */
    public QuerySession requireLiveSession() {
        return execution.requireLiveSession();
    }
}
