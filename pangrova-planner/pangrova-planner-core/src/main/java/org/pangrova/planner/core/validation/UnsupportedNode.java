package org.pangrova.planner.core.validation;

/**
 * Describes a single Substrait node that is not supported by the native engine.
 *
 * @param nodeType simple class name of the unsupported Rel or Expression
 * @param reason   human-readable explanation of why the node is unsupported
 * @param depth    depth in the Rel/Expression tree (root = 0)
 * @param kind     typed capability failure, independent of diagnostic wording
 */
public record UnsupportedNode(
        String nodeType,
        String reason,
        int depth,
        Kind kind
) {
    public enum Kind { RELATION, EXPRESSION, FUNCTION }
}
