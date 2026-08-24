package org.pangrova.coordinator.query.statement.dml;

import java.util.Map;
import java.util.function.Function;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.IsNullExpression;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.spi.connector.metadata.DeleteFilter;

import java.util.List;

/**
 * Converts parser AST WHERE expressions to connector-agnostic DeleteFilter.
 * Covers common DELETE WHERE patterns: comparisons, AND/OR, NOT, IS NULL.
 */
public final class DeleteFilterConverter {

    private DeleteFilterConverter() {}

    public static DeleteFilter convert(Expression expr) {
        if (expr instanceof BinaryExpression bin) {
            return convertBinary(bin);
        }
        if (expr instanceof IsNullExpression isNull) {
            String col = extractColumnName(isNull.operand());
            return isNull.negated()
                    ? new DeleteFilter.IsNotNull(col)
                    : new DeleteFilter.IsNull(col);
        }
        if (expr instanceof NotExpression not) {
            return new DeleteFilter.Not(convert(not.expression()));
        }
        throw new UnsupportedOperationException(
                "Unsupported DELETE WHERE expression: " + expr.getClass().getSimpleName());
    }

    private static DeleteFilter convertBinary(BinaryExpression bin) {
        return switch (bin.operator()) {
            case AND -> new DeleteFilter.And(List.of(convert(bin.left()), convert(bin.right())));
            case OR -> new DeleteFilter.Or(List.of(convert(bin.left()), convert(bin.right())));
            case EQUAL -> comparison(bin, DeleteFilter.Equal::new);
            case NOT_EQUAL -> comparison(bin, DeleteFilter.NotEqual::new);
            case LESS_THAN -> comparison(bin, DeleteFilter.LessThan::new);
            case LESS_THAN_OR_EQUAL -> comparison(bin, DeleteFilter.LessThanOrEqual::new);
            case GREATER_THAN -> comparison(bin, DeleteFilter.GreaterThan::new);
            case GREATER_THAN_OR_EQUAL -> comparison(bin, DeleteFilter.GreaterThanOrEqual::new);
            default -> throw new UnsupportedOperationException(
                    "Unsupported DELETE WHERE operator: " + bin.operator());
        };
    }

    private static DeleteFilter comparison(BinaryExpression bin,
                                           ComparisonFactory factory) {
        String column = extractColumnName(bin.left());
        Object value = extractLiteralValue(bin.right());
        return factory.create(column, value);
    }

    private static String extractColumnName(Expression expr) {
        if (expr instanceof Identifier id) {
            return id.parts().get(id.parts().size() - 1);
        }
        throw new UnsupportedOperationException(
                "Expected column reference, got: " + expr.getClass().getSimpleName());
    }

    private static Object extractLiteralValue(Expression expr) {
        if (!(expr instanceof Literal lit)) {
            throw new UnsupportedOperationException(
                    "Expected literal value, got: " + expr.getClass().getSimpleName());
        }
        return unwrapLiteralValue(lit.value());
    }

    private static final Map<Class<?>, Function<LiteralValue, Object>>
            LITERAL_UNWRAPPERS = Map.of(
                    LiteralValue.IntegerValue.class,
                    v -> ((LiteralValue.IntegerValue) v).value(),
                    LiteralValue.DecimalValue.class,
                    v -> ((LiteralValue.DecimalValue) v).value(),
                    LiteralValue.StringValue.class,
                    v -> ((LiteralValue.StringValue) v).value(),
                    LiteralValue.BooleanValue.class,
                    v -> ((LiteralValue.BooleanValue) v).value(),
                    LiteralValue.NullValue.class,
                    v -> null,
                    LiteralValue.DateValue.class,
                    v -> ((LiteralValue.DateValue) v).value(),
                    LiteralValue.TimestampValue.class,
                    v -> ((LiteralValue.TimestampValue) v).value(),
                    LiteralValue.IntervalValue.class,
                    v -> ((LiteralValue.IntervalValue) v).expression());

    private static Object unwrapLiteralValue(LiteralValue v) {
        var unwrapper = LITERAL_UNWRAPPERS.get(v.getClass());
        if (unwrapper == null) {
            throw new UnsupportedOperationException(
                    "Unknown literal type: " + v.getClass().getSimpleName());
        }
        return unwrapper.apply(v);
    }

    @FunctionalInterface
    private interface ComparisonFactory {
        DeleteFilter create(String column, Object value);
    }
}
