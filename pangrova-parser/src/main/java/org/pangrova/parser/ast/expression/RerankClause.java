package org.pangrova.parser.ast.expression;

import java.util.List;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;

/**
 * Represents a RERANK BY clause in a SELECT query.
 *
 * <p>Supports reranking strategies: rrf(k), linear_combination(w1, w2),
 * cross_encoder('model', top_n), and cascade(...) for chaining multiple strategies.
 *
 * <p>Optional ON (col1, col2, ...) specifies multi-vector column targets.
 */
public record RerankClause(RerankStrategy strategy, List<Identifier> onColumns) {

    public RerankClause {
        onColumns = List.copyOf(onColumns);
    }

    public RerankClause(RerankStrategy strategy) {
        this(strategy, List.of());
    }

    /**
     * Base type for all reranking strategies.
     */
    public sealed interface RerankStrategy
            permits RrfStrategy, LinearCombinationStrategy, CrossEncoderStrategy, CascadeStrategy {}

    /**
     * Reciprocal Rank Fusion: rrf(k).
     *
     * @param k the fusion constant (typically 60)
     */
    public record RrfStrategy(Expression k) implements RerankStrategy {}

    /**
     * Weighted linear combination of scores: linear_combination(w1, w2).
     *
     * @param weight1 weight for the first score source (e.g., vector similarity)
     * @param weight2 weight for the second score source (e.g., BM25)
     */
    public record LinearCombinationStrategy(Expression weight1, Expression weight2) implements RerankStrategy {}

    /**
     * Cross-encoder model reranking: cross_encoder('model_name', top_n).
     *
     * @param modelName the cross-encoder model identifier
     * @param topN number of top candidates to rerank
     */
    public record CrossEncoderStrategy(String modelName, Expression topN) implements RerankStrategy {}

    /**
     * Cascade of multiple reranking strategies applied sequentially: cascade(s1, s2, ...).
     *
     * @param stages ordered list of reranking strategies to apply in sequence
     */
    public record CascadeStrategy(List<RerankStrategy> stages) implements RerankStrategy {
        public CascadeStrategy {
            stages = List.copyOf(stages);
        }
    }
}
