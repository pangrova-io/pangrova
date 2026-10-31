package org.pangrova.parser.ast.expression;

import java.util.List;
import java.util.Optional;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

/**
 * Represents CASE expressions (simple and searched).
 * Simple: CASE expr WHEN val THEN result ... [ELSE default] END
 * Searched: CASE WHEN condition THEN result ... [ELSE default] END
 */
public record CaseExpression(
        Optional<Expression> operand,
        List<WhenClause> whenClauses,
        Optional<Expression> elseResult) implements Expression {

    public CaseExpression {
        whenClauses = List.copyOf(whenClauses);
    }

    public record WhenClause(Expression condition, Expression result) {}

    @Override
    public ExpressionType type() {
        return ExpressionType.CASE;
    }
}
