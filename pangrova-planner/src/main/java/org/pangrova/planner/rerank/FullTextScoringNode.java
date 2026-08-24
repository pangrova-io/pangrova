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
 */
public record FullTextScoringNode(
        String tableName,
        String column,
        String queryText,
        List<String> serializedPreFilters) {

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
    }
}
