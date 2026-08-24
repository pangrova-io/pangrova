package org.pangrova.spi.index.vector;

import org.pangrova.spi.annotation.Experimental;

/**
 * Strategy for combining scalar predicates with ANN (Approximate Nearest
 * Neighbor) vector search.
 *
 * <p>Traditional ANN search returns the top-K nearest neighbors without
 * considering row-level filter predicates. When a SQL query combines a
 * vector similarity search with WHERE-clause filters, the engine must
 * choose how to interleave filtering and ANN traversal. Each strategy
 * represents a different trade-off between recall, latency, and
 * implementation complexity.</p>
 *
 * <p>The default is {@link #POST_FILTER}, which preserves backward
 * compatibility with the existing coarse post-filter / brute-force
 * heuristic behavior. Callers can select a strategy per-query via
 * session properties or query hints.</p>
 *
 * <p>Industry references:
 * <ul>
 *   <li>Milvus: pre-filter, post-filter, and iterative scan</li>
 *   <li>Pinecone: metadata filtering integrated into HNSW traversal</li>
 *   <li>Weaviate: pre-filter with ACORN-style predicate-aware HNSW</li>
 *   <li>pgvector / Lantern: iterative scan with index-backed re-ranking</li>
 * </ul>
 * </p>
 *
 * @see VectorSearchRequest
 * @see VectorHybridSearchStrategy
 */
@Experimental(reason = "G-AI-06: filtered ANN search strategies")
public enum FilteredSearchStrategy {

    /**
     * Run full ANN search first, then discard rows that fail the scalar
     * predicate. Simple but loses recall when filter selectivity is high
     * (many rows filtered out). Over-fetching (effectiveK &gt; topK) can
     * partially compensate.
     *
     * <p>This is the current default behavior and preserves backward
     * compatibility.</p>
     */
    POST_FILTER,

    /**
     * Evaluate scalar predicates first to build a candidate row set, then
     * perform brute-force or ANN search only within that set. Best when
     * the predicate is highly selective (few rows survive) so the
     * brute-force pass is cheap.
     */
    PRE_FILTER,

    /**
     * Evaluate the filter predicate inline during HNSW graph traversal,
     * skipping neighbors that fail the predicate without expanding them.
     * Achieves high recall without over-fetching, but requires tight
     * integration between the graph walker and the predicate evaluator.
     *
     * <p>Most effective at moderate selectivity (20-80% of rows survive
     * the predicate). At very low selectivity the graph traversal may
     * degenerate because too many neighbors are pruned.</p>
     */
    PREDICATE_AWARE,

    /**
     * Iterative scan: retrieve batches of ANN candidates, apply the filter,
     * and continue fetching more candidates until the requested topK
     * filtered results are collected or the candidate pool is exhausted.
     *
     * <p>Adapts dynamically to any selectivity level. Slightly higher
     * latency than single-pass approaches due to multiple index probes,
     * but provides the best recall guarantees across all selectivity
     * ranges.</p>
     */
    ITERATIVE_SCAN;

    /**
     * Returns the default strategy used when none is explicitly specified.
     *
     * @return {@link #POST_FILTER}
     */
    public static FilteredSearchStrategy defaultStrategy() {
        return POST_FILTER;
    }
}
