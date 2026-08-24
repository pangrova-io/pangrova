package org.pangrova.coordinator.ai.training;

/**
 * Thrown when a split expression does not conform to the restricted predicate grammar.
 *
 * <p>Maps to error code INVALID_SPLIT_EXPRESSION (3014). Only hash-based modulo
 * predicates are allowed; arbitrary SQL is rejected.</p>
 */
public final class InvalidSplitExpressionException extends RuntimeException {

    private static final int ERROR_CODE = 3014;

    private final String expression;

    /**
     * Creates an exception for an invalid split expression.
     *
     * @param expression the invalid expression
     * @param reason     why the expression is invalid
     */
    public InvalidSplitExpressionException(String expression, String reason) {
        super("INVALID_SPLIT_EXPRESSION (3014): " + reason + " [expression=" + expression + "]");
        this.expression = expression;
    }

    /**
     * Returns the error code for this exception type.
     *
     * @return 3014
     */
    public int errorCode() {
        return ERROR_CODE;
    }

    /**
     * Returns the invalid expression that caused this exception.
     *
     * @return the expression string
     */
    public String expression() {
        return expression;
    }
}
