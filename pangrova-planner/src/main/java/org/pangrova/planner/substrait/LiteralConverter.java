package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.type.Type;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.apache.calcite.rex.RexLiteral;

import java.math.BigDecimal;

/**
 * Converts Calcite RexLiteral values to Substrait Expression literals.
 * Handles null, numeric, string, boolean, temporal and binary literals.
 */
final class LiteralConverter {

    private final TypeMapper typeMapper;

    LiteralConverter(TypeMapper typeMapper) {
        this.typeMapper = typeMapper;
    }

    Expression convert(RexLiteral literal) {
        if (literal.isNull()) {
            return buildNullLiteral(literal);
        }
        return convertNonNullLiteral(literal);
    }

    private Expression buildNullLiteral(RexLiteral literal) {
        Type nullType = typeMapper.map(literal.getType());
        return ImmutableExpression.NullLiteral.builder().type(nullType).build();
    }

    private Expression convertNonNullLiteral(RexLiteral literal) {
        boolean nullable = literal.getType().isNullable();
        return switch (literal.getTypeName()) {
            case INTEGER -> ExpressionCreator.i32(nullable, literal.getValueAs(Integer.class));
            case BIGINT -> ExpressionCreator.i64(nullable, literal.getValueAs(Long.class));
            case VARCHAR, CHAR -> ExpressionCreator.string(nullable, literal.getValueAs(String.class));
            case BOOLEAN -> ExpressionCreator.bool(nullable, literal.getValueAs(Boolean.class));
            case DOUBLE, FLOAT -> ExpressionCreator.fp64(nullable, literal.getValueAs(Double.class));
            case DECIMAL -> convertDecimalLiteral(literal, nullable);
            case DATE -> convertDateLiteral(literal, nullable);
            case TIME -> convertTimeLiteral(literal, nullable);
            case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> convertTimestampLiteral(literal, nullable);
            case BINARY, VARBINARY -> convertBinaryLiteral(literal, nullable);
            case NULL -> buildNullLiteral(literal);
            default -> throw unsupported("literal type: " + literal.getTypeName());
        };
    }

    private Expression convertDecimalLiteral(RexLiteral literal, boolean nullable) {
        BigDecimal value = literal.getValueAs(BigDecimal.class);
        int precision = literal.getType().getPrecision();
        int scale = literal.getType().getScale();
        // Guard against Calcite REDUCE_EXPRESSIONS producing invalid precision/scale
        // (e.g. Integer.MIN_VALUE) — derive sane values from the BigDecimal itself.
        if (precision <= 0 || scale < 0 || precision > 38 || scale > precision) {
            scale = Math.max(0, value.scale());
            precision = Math.max(1, value.precision());
            if (precision < scale) {
                precision = scale + 1;
            }
            if (precision > 38) {
                precision = 38;
                scale = Math.min(scale, 38);
            }
        }
        return ExpressionCreator.decimal(nullable, value, precision, scale);
    }

    private Expression convertDateLiteral(RexLiteral literal, boolean nullable) {
        // Calcite stores DATE as days since epoch
        int daysSinceEpoch = literal.getValueAs(Integer.class);
        return ExpressionCreator.date(nullable, daysSinceEpoch);
    }

    private Expression convertTimeLiteral(RexLiteral literal, boolean nullable) {
        // Calcite stores TIME as milliseconds since midnight; Substrait uses microseconds
        int millis = literal.getValueAs(Integer.class);
        long micros = millis * 1000L;
        return ExpressionCreator.time(nullable, micros);
    }

    private Expression convertTimestampLiteral(RexLiteral literal, boolean nullable) {
        // Calcite stores TIMESTAMP as milliseconds since epoch; Substrait uses microseconds
        long millis = literal.getValueAs(Long.class);
        long micros = millis * 1000L;
        return ExpressionCreator.timestamp(nullable, micros);
    }

    private Expression convertBinaryLiteral(RexLiteral literal, boolean nullable) {
        byte[] bytes = literal.getValueAs(byte[].class);
        return ExpressionCreator.binary(nullable, bytes);
    }

    private static SubstraitConversionException unsupported(String detail) {
        return new SubstraitConversionException(
                PangrovaErrorCode.SUBSTRAIT_UNSUPPORTED_PLAN_NODE,
                "Unsupported " + detail);
    }
}
