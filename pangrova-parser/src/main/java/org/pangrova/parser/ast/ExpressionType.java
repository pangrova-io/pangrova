package org.pangrova.parser.ast;

/**
 * Enumerates all concrete expression types in the AST hierarchy.
 *
 * <p>Enables exhaustive switch expressions over {@link Expression} subtypes,
 * providing compile-time completeness checking. Adding a new expression type
 * requires adding a corresponding enum value here; any switch expression that
 * does not cover the new value will fail to compile.</p>
 */
public enum ExpressionType {
    IDENTIFIER,
    LITERAL,
    ARRAY_CONSTRUCTOR,
    MAP_CONSTRUCTOR,
    STRUCT_CONSTRUCTOR,
    BETWEEN,
    BINARY,
    CASE,
    CAST,
    EXISTS,
    EXTRACT,
    FUNCTION_CALL,
    IN,
    IN_SUBQUERY,
    IS_NULL,
    TRUTH_TEST,
    LIKE,
    NOT,
    NOT_IN,
    NOT_IN_SUBQUERY,
    STAR_FUNCTION_CALL,
    SUBQUERY,
    SUBSTRING,
    UNARY_MINUS,
    WINDOWED_FUNCTION_CALL,
    PARAMETER_MARKER,
    SUBSCRIPT
}
