package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Applies implicit argument type coercion for scalar function calls.
 * Calcite constant folding often represents integer literals as DECIMAL(p,0),
 * while the native engine requires integer-typed arguments and matching
 * comparison operand types.
 */
final class ComparisonArgCoercer {

    private static final Set<SqlKind> COMPARISON_KINDS = Set.of(
            SqlKind.EQUALS, SqlKind.NOT_EQUALS,
            SqlKind.LESS_THAN, SqlKind.GREATER_THAN,
            SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN_OR_EQUAL);

    private final Function<RexNode, Expression> converter;

    ComparisonArgCoercer(Function<RexNode, Expression> converter) {
        this.converter = converter;
    }

    /**
     * Converts arguments for a scalar function call, applying implicit type
     * coercion for comparison operators when Calcite produces DECIMAL(p,0)
     * literals against integer columns. The native engine requires both sides of a
     * comparison to share the same type; Calcite's constant folding often
     * represents integer literals as DECIMAL(10,0).
     */
    List<Expression> convertComparisonArgs(RexCall call) {
        List<RexNode> operands = call.getOperands();
        if (!COMPARISON_KINDS.contains(call.getKind()) || operands.size() != 2) {
            return operands.stream().map(converter).toList();
        }
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);
        Expression leftExpr = converter.apply(left);
        Expression rightExpr = converter.apply(right);
        return coerceDecimalToInteger(left, right, leftExpr, rightExpr);
    }

    /**
     * Coerces any DECIMAL(p,0) literal arguments to I32 for non-comparison
     * scalar functions (e.g. SUBSTRING). Calcite constant-folds CAST(1 AS INTEGER)
     * back to DECIMAL(10,0), but the native engine expects integer arguments.
     */
    List<Expression> coerceDecimalLiteralsToInteger(
            List<RexNode> operands, List<Expression> converted) {
        boolean changed = false;
        Expression[] result = converted.toArray(Expression[]::new);
        for (int i = 0; i < operands.size(); i++) {
            if (operands.get(i) instanceof RexLiteral lit
                    && lit.getTypeName() == SqlTypeName.DECIMAL
                    && lit.getType().getScale() == 0) {
                Expression coerced = coerceSingleDecimal(lit);
                if (coerced != null) {
                    result[i] = coerced;
                    changed = true;
                }
            }
        }
        return changed ? List.of(result) : converted;
    }

    /**
     * If one operand is an integer column and the other is a DECIMAL(p,0) literal,
     * replaces the DECIMAL literal with an integer literal of the matching type.
     */
    private List<Expression> coerceDecimalToInteger(
            RexNode leftNode, RexNode rightNode,
            Expression leftExpr, Expression rightExpr) {
        Expression coercedRight = tryCoerceDecimalLiteral(leftNode, rightNode, rightExpr);
        if (coercedRight != rightExpr) {
            return List.of(leftExpr, coercedRight);
        }
        Expression coercedLeft = tryCoerceDecimalLiteral(rightNode, leftNode, leftExpr);
        if (coercedLeft != leftExpr) {
            return List.of(coercedLeft, rightExpr);
        }
        return List.of(leftExpr, rightExpr);
    }

    /**
     * If columnNode is an integer type and literalNode is a DECIMAL(p,0) literal,
     * returns a new integer literal matching the column type.
     */
    private Expression tryCoerceDecimalLiteral(
            RexNode columnNode, RexNode literalNode, Expression literalExpr) {
        if (!(literalNode instanceof RexLiteral lit)
                || lit.getTypeName() != SqlTypeName.DECIMAL
                || lit.getType().getScale() != 0) {
            return literalExpr;
        }
        BigDecimal decimalValue = lit.getValueAs(BigDecimal.class);
        if (decimalValue == null) {
            return literalExpr;
        }
        boolean nullable = lit.getType().isNullable();
        return switch (columnNode.getType().getSqlTypeName()) {
            case BIGINT -> ExpressionCreator.i64(nullable, decimalValue.longValueExact());
            case INTEGER -> ExpressionCreator.i32(nullable, decimalValue.intValueExact());
            case SMALLINT -> ExpressionCreator.i16(nullable, decimalValue.shortValueExact());
            case TINYINT -> ExpressionCreator.i8(nullable, decimalValue.byteValueExact());
            default -> literalExpr;
        };
    }

    /**
     * Coerces a single DECIMAL(p,0) literal to an integer literal (i32 or i64).
     * Returns {@code null} when the literal value is null and no coercion is possible.
     */
    private Expression coerceSingleDecimal(RexLiteral lit) {
        BigDecimal val = lit.getValueAs(BigDecimal.class);
        if (val == null) {
            return null;
        }
        boolean nullable = lit.getType().isNullable();
        // Use i64 for values outside int range to avoid ArithmeticException
        try {
            return ExpressionCreator.i32(nullable, val.intValueExact());
        } catch (ArithmeticException overflow) {
            return ExpressionCreator.i64(nullable, val.longValueExact());
        }
    }
}
