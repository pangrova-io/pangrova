package org.pangrova.planner.rerank;

import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a full-text scoring operation via MATCH().
 *
 * <p>Produced when the planner detects {@code MATCH(column, 'query')} in WHERE or SELECT.
 * The node carries the column, query text, and optional pre-filter predicates needed
 * to compute BM25 scores at execution time.</p>
 *
 * <p>Results are (row_id, BM25_score) pairs suitable for fusion with vector search
 * results via RRF or other reranking strategies.</p>
 *
 * @param tableName            fully qualified table name
 * @param column               column with inverted index for FTS scoring
 * @param queryText            raw query text for BM25 scoring
 * @param serializedPreFilters additional WHERE-clause filters to apply before scoring
 * @param indexId              bound inverted-index identifier; empty for exact fallback
 * @param indexGeneration      bound index generation; -1 when unavailable
 * @param sourceSnapshot       query source snapshot; empty when unavailable
 * @param indexSnapshot        indexed source snapshot/generation; empty when unavailable
 * @param indexFreshnessEpochMs index metadata freshness timestamp; zero when unavailable
 */
public record FullTextScoringNode(
        String tableName,
        String column,
        String queryText,
        List<String> serializedPreFilters,
        String indexId,
        int indexGeneration,
        String sourceSnapshot,
        String indexSnapshot,
        long indexFreshnessEpochMs) {

    /** Creates the planner-owned intent before control-plane index binding. */
    public FullTextScoringNode(
            String tableName, String column, String queryText,
            List<String> serializedPreFilters) {
        this(tableName, column, queryText, serializedPreFilters,
                "", -1, "", "", 0);
    }

    public FullTextScoringNode {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(queryText, "queryText");
        if (queryText.isBlank()) {
            throw new IllegalArgumentException("queryText must not be blank");
        }
        serializedPreFilters = serializedPreFilters == null
                ? List.of()
                : List.copyOf(serializedPreFilters);
        indexId = indexId == null ? "" : indexId;
        sourceSnapshot = sourceSnapshot == null ? "" : sourceSnapshot;
        indexSnapshot = indexSnapshot == null ? "" : indexSnapshot;
        if (indexGeneration < -1) {
            throw new IllegalArgumentException("indexGeneration must be >= -1");
        }
        if (indexFreshnessEpochMs < 0) {
            throw new IllegalArgumentException("indexFreshnessEpochMs must be non-negative");
        }
    }

    /** Returns a copy with the READY index chosen by the Coordinator. */
    public FullTextScoringNode withIndexBinding(
            String newIndexId,
            int newGeneration,
            String newSourceSnapshot,
            String newIndexSnapshot,
            long newFreshnessEpochMs) {
        return new FullTextScoringNode(
                tableName, column, queryText, serializedPreFilters,
                newIndexId, newGeneration, newSourceSnapshot, newIndexSnapshot,
                newFreshnessEpochMs);
    }
}
