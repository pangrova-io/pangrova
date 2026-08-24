package org.pangrova.parser.ast;

import java.util.Optional;

import org.pangrova.parser.ast.expression.ArrayConstructor;
import org.pangrova.parser.ast.expression.BetweenExpression;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.CaseExpression;
import org.pangrova.parser.ast.expression.CastExpression;
import org.pangrova.parser.ast.expression.ExistsExpression;
import org.pangrova.parser.ast.expression.ExtractExpression;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.parser.ast.expression.InExpression;
import org.pangrova.parser.ast.expression.InSubqueryExpression;
import org.pangrova.parser.ast.expression.IsNullExpression;
import org.pangrova.parser.ast.expression.LikeExpression;
import org.pangrova.parser.ast.expression.MapConstructor;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.parser.ast.expression.NotInExpression;
import org.pangrova.parser.ast.expression.NotInSubqueryExpression;
import org.pangrova.parser.ast.expression.StarFunctionCall;
import org.pangrova.parser.ast.expression.StructConstructor;
import org.pangrova.parser.ast.expression.SubqueryExpression;
import org.pangrova.parser.ast.expression.SubstringExpression;
import org.pangrova.parser.ast.expression.UnaryMinusExpression;
import org.pangrova.parser.ast.expression.WindowedFunctionCall;

/**
 * Base interface for all expression AST nodes.
 *
 * <p>The {@link #type()} method enables exhaustive switch expressions with
 * compile-time completeness checking via {@link ExpressionType}. Adding a new
 * expression type requires adding a corresponding enum value to {@link ExpressionType}
 * and a case to the {@code type()} default implementation; any switch expression
 * that does not cover the new value will fail to compile.</p>
 */
public interface Expression {

    /**
     * Returns the source location of this expression in the original SQL text.
     * Default returns empty; populated via {@link SourceLocationRegistry} during parsing.
     *
     * @return source location if available, empty otherwise
     */
    default Optional<SourceLocation> sourceLocation() {
        return Optional.empty();
    }

    /**
     * Returns the concrete type of this expression for exhaustive switch dispatch.
     *
     * @throws IllegalStateException if the expression type is not recognized
     */
    default ExpressionType type() {
        if (this instanceof Identifier) return ExpressionType.IDENTIFIER;
        if (this instanceof Literal) return ExpressionType.LITERAL;
        if (this instanceof ArrayConstructor) return ExpressionType.ARRAY_CONSTRUCTOR;
        if (this instanceof MapConstructor) return ExpressionType.MAP_CONSTRUCTOR;
        if (this instanceof StructConstructor) return ExpressionType.STRUCT_CONSTRUCTOR;
        if (this instanceof BetweenExpression) return ExpressionType.BETWEEN;
        if (this instanceof BinaryExpression) return ExpressionType.BINARY;
        if (this instanceof CaseExpression) return ExpressionType.CASE;
        if (this instanceof CastExpression) return ExpressionType.CAST;
        if (this instanceof ExistsExpression) return ExpressionType.EXISTS;
        if (this instanceof ExtractExpression) return ExpressionType.EXTRACT;
        if (this instanceof FunctionCall) return ExpressionType.FUNCTION_CALL;
        if (this instanceof InExpression) return ExpressionType.IN;
        if (this instanceof InSubqueryExpression) return ExpressionType.IN_SUBQUERY;
        if (this instanceof IsNullExpression) return ExpressionType.IS_NULL;
        if (this instanceof LikeExpression) return ExpressionType.LIKE;
        if (this instanceof NotExpression) return ExpressionType.NOT;
        if (this instanceof NotInExpression) return ExpressionType.NOT_IN;
        if (this instanceof NotInSubqueryExpression) return ExpressionType.NOT_IN_SUBQUERY;
        if (this instanceof StarFunctionCall) return ExpressionType.STAR_FUNCTION_CALL;
        if (this instanceof SubqueryExpression) return ExpressionType.SUBQUERY;
        if (this instanceof SubstringExpression) return ExpressionType.SUBSTRING;
        if (this instanceof UnaryMinusExpression) return ExpressionType.UNARY_MINUS;
        if (this instanceof WindowedFunctionCall) return ExpressionType.WINDOWED_FUNCTION_CALL;
        if (this instanceof org.pangrova.parser.ast.expression.ParameterMarker) return ExpressionType.PARAMETER_MARKER;
        throw new IllegalStateException("Unknown expression type: " + getClass().getName());
    }
}
