package org.pangrova.parser.api.ast;

/**
 * A plain expression in a GROUP BY clause.
 *
 * <p>Wraps a single {@link Expression} to conform to the
 * {@link GroupByItem} sealed hierarchy. This is the most common
 * case: {@code GROUP BY a, b} produces two {@code GroupByExpression}
 * instances.</p>
 *
 * @param expression the grouping expression (column reference, function call, etc.)
 */
public record GroupByExpression(Expression expression) implements GroupByItem {

    public GroupByExpression {
        if (expression == null) {
            throw new IllegalArgumentException("expression must not be null");
        }
    }
}
