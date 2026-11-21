package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.TableSamplingMethod;

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
        TableSamplingMethod method,
        Expression percentage,
        Optional<Expression> seed) {

}
