package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.ExpressionType;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;

/**
 * Base contract for SQL expression AST nodes.
 *
 * <p>Built-in nodes declare their own {@link #type()} using {@link ExpressionType}.
 * Exhaustive switches over that enum must account for every built-in kind.
 * Extension nodes may implement this contract without claiming a built-in type.</p>
 */
@Experimental
public interface Expression {
    /**
     * Returns an embedded source location, if supplied by this node.
     * Parser-owned location tables belong to implementation-specific diagnostic tooling.
     */
    default Optional<SourceLocation> sourceLocation() {
        return Optional.empty();
    }

    /**
     * Returns this node's built-in kind for exhaustive dispatch.
     *
     * @throws IllegalStateException if an extension does not declare a built-in kind
     */
    default ExpressionType type() {
        throw new IllegalStateException("Unknown expression type: " + getClass().getName());
    }
}
