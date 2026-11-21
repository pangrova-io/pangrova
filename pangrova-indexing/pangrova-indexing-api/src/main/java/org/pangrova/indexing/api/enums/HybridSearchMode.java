package org.pangrova.indexing.api.enums;

/** Evaluation order of filtering and vector search. */
public enum HybridSearchMode {
    /** Run ANN search first with over-fetched K, then apply post-filter. */
    ANN_THEN_FILTER,
    /** Apply pre-filter first to reduce candidate set, then brute-force scan survivors. */
    FILTER_THEN_BRUTEFORCE,
    /**
     * Filtered graph traversal: evaluate the pre-filter inline during HNSW
     * graph traversal, skipping neighbors that fail the filter predicate.
     * Most efficient when selectivity is moderate (20-80% of rows survive).
     */
    FILTERED_ANN
}
