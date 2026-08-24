package org.pangrova.planner.rerank;

import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a reranking operation that fuses scored results
 * from one or more retrieval sources (vector search, full-text search) into a
 * single ranked result set.
 *
 * <p>Produced by {@link RerankerInjectionRule} when the planner detects a RERANK BY
 * clause or a hybrid search pattern (vector + FTS without explicit RERANK BY).
 * Consumed by the Worker's reranker executors (RRF, Weighted, CrossEncoder, Cascade).</p>
 *
 * <p>The node is positioned below projections in the plan tree so that columns
 * needed for scoring (distance, BM25) remain available during reranking, even if
 * the final SELECT does not include them.</p>
 *
 * @param strategy       the reranking strategy descriptor (RRF, linear combination, etc.)
 * @param sources        retrieval sources providing scored candidates for fusion
 * @param topK           maximum number of results to return after reranking
 * @param queryText      original query text for cross-encoder scoring (may be null for pure vector)
 * @param targetColumns  multi-vector ON columns; empty for default single-source or hybrid
 */
public record RerankerPlanNode(
        RerankerStrategy strategy,
        List<RetrievalSource> sources,
        int topK,
        String queryText,
        List<String> targetColumns) {

    public RerankerPlanNode {
        Objects.requireNonNull(strategy, "strategy");
        Objects.requireNonNull(sources, "sources");
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("sources must not be empty");
        }
        sources = List.copyOf(sources);
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive, got: " + topK);
        }
        targetColumns = targetColumns == null ? List.of() : List.copyOf(targetColumns);
    }

    /**
     * Returns true if this node fuses results from multiple retrieval sources.
     */
    public boolean isHybrid() {
        return sources.size() > 1;
    }

    /**
     * Returns true if this node operates on multiple vector columns (ON clause).
     */
    public boolean isMultiVector() {
        return targetColumns.size() > 1;
    }

    /**
     * Describes the reranking strategy to apply.
     */
    public sealed interface RerankerStrategy
            permits RrfStrategy, LinearCombinationStrategy, CrossEncoderStrategy, CascadeStrategy {}

    /**
     * Reciprocal Rank Fusion with configurable k parameter.
     *
     * @param k the fusion constant (higher k reduces the influence of high-ranked items)
     */
    public record RrfStrategy(int k) implements RerankerStrategy {
        public RrfStrategy {
            if (k <= 0) {
                throw new IllegalArgumentException("k must be positive, got: " + k);
            }
        }
    }

    /**
     * Weighted linear combination of normalized scores from two sources.
     *
     * @param weight1 weight for the first source (typically vector similarity)
     * @param weight2 weight for the second source (typically BM25)
     */
    public record LinearCombinationStrategy(double weight1, double weight2) implements RerankerStrategy {
        public LinearCombinationStrategy {
            if (weight1 < 0 || weight2 < 0) {
                throw new IllegalArgumentException(
                        "weights must be non-negative, got: w1=" + weight1 + ", w2=" + weight2);
            }
        }
    }

    /**
     * Cross-encoder model reranking that jointly scores query-document pairs.
     *
     * @param modelName the registered cross-encoder model identifier
     * @param topN      number of top candidates to pass through the cross-encoder
     */
    public record CrossEncoderStrategy(String modelName, int topN) implements RerankerStrategy {
        public CrossEncoderStrategy {
            Objects.requireNonNull(modelName, "modelName");
            if (modelName.isBlank()) {
                throw new IllegalArgumentException("modelName must not be blank");
            }
            if (topN <= 0) {
                throw new IllegalArgumentException("topN must be positive, got: " + topN);
            }
        }
    }

    /**
     * Cascade of multiple reranking strategies applied sequentially.
     * Each stage refines the results of the previous stage.
     *
     * @param stages ordered list of strategies to apply in sequence
     */
    public record CascadeStrategy(List<RerankerStrategy> stages) implements RerankerStrategy {
        public CascadeStrategy {
            Objects.requireNonNull(stages, "stages");
            if (stages.size() < 2) {
                throw new IllegalArgumentException("cascade requires at least 2 stages, got: " + stages.size());
            }
            stages = List.copyOf(stages);
        }
    }

    /**
     * Describes a retrieval source providing scored candidates for reranking.
     */
    public sealed interface RetrievalSource permits VectorSource, FullTextSource {}

    /**
     * Vector similarity search source producing distance-based scores.
     *
     * @param column      the vector column name
     * @param queryVector the query embedding (defensively copied)
     * @param metric      distance metric name (cosine_distance, l2_distance, inner_product)
     */
    public record VectorSource(String column, float[] queryVector, String metric) implements RetrievalSource {
        public VectorSource {
            Objects.requireNonNull(column, "column");
            Objects.requireNonNull(queryVector, "queryVector");
            Objects.requireNonNull(metric, "metric");
            queryVector = queryVector.clone();
        }
    }

    /**
     * Full-text search source producing BM25 relevance scores.
     *
     * @param scoringNode the FTS scoring plan node with column, query, and pre-filters
     */
    public record FullTextSource(FullTextScoringNode scoringNode) implements RetrievalSource {
        public FullTextSource {
            Objects.requireNonNull(scoringNode, "scoringNode");
        }
    }
}
