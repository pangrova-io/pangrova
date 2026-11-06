package org.pangrova.indexing.api.vector;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Execution strategy for hybrid vector search (pre-filter + ANN).
 *
 * <p>Defined in the indexing contract artifact because the Worker needs to know the
 * execution mode, and the Worker cannot depend on {@code pangrova-planner}.</p>
 *
 * @param mode       execution mode (ANN first then filter, or filter first then brute-force)
 * @param effectiveK effective K value used during search (may differ from user-specified topK
 *                   due to over-fetching in ANN_THEN_FILTER mode)
 */
@Experimental
public record VectorHybridSearchStrategy(Mode mode, int effectiveK) {

    /** Hybrid search execution modes. */
    public enum Mode {
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
}
