package org.pangrova.parser.ast;

import java.util.Optional;

/**
 * Represents a TABLESAMPLE clause attached to a table reference.
 *
 * <p>Supports BERNOULLI (row-level) and SYSTEM (block-level) sampling methods,
 * with an optional REPEATABLE seed for deterministic results.
 *
 * <p>Example: {@code SELECT * FROM orders TABLESAMPLE BERNOULLI(10) REPEATABLE(42)}
 */
public record TableSample(
        SamplingMethod method,
        Expression percentage,
        Optional<Expression> seed) {

    /**
     * Sampling method for TABLESAMPLE clause.
     */
    public enum SamplingMethod {
        /** Row-level sampling: each row independently included with given probability. */
        BERNOULLI,
        /** Block-level sampling: entire blocks selected with given probability. */
        SYSTEM
    }
}
