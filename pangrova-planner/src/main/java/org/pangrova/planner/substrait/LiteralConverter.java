package org.pangrova.planner.substrait;

import org.pangrova.planner.exception.SubstraitConversionException;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.type.Type;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimeString;

import java.math.BigDecimal;
import java.math.RoundingMode;

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
        return convert(literal, literal.getType());
    }

    /**
     * Converts a literal in its declared expression context, preserving the
     * target type parameters and rejecting lossy integer/decimal conversions.
     */
    Expression convert(RexLiteral literal, RelDataType fieldType) {
        if (literal.isNull()) {
            if (!fieldType.isNullable()) {
                throw new SubstraitConversionException(
                        PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                        "NULL value in non-nullable literal context");
            }
            return buildNullLiteral(fieldType);
        }
        return convertNonNullLiteral(literal, fieldType);
    }

    private Expression buildNullLiteral(RelDataType type) {
        Type nullType = typeMapper.map(type);
        return ImmutableExpression.NullLiteral.builder().type(nullType).build();
    }

    private Expression convertNonNullLiteral(RexLiteral literal, RelDataType fieldType) {
        boolean nullable = fieldType.isNullable();
        // The expression context may widen a type independently of its literal.
        try {
            return switch (fieldType.getSqlTypeName()) {
                case TINYINT -> ExpressionCreator.i8(nullable, numeric(literal).byteValueExact());
                case SMALLINT -> ExpressionCreator.i16(nullable, numeric(literal).shortValueExact());
                case INTEGER -> ExpressionCreator.i32(nullable, numeric(literal).intValueExact());
                case BIGINT -> ExpressionCreator.i64(nullable, numeric(literal).longValueExact());
                case VARCHAR, CHAR -> ExpressionCreator.string(nullable,
                        requireValue(literal, String.class));
                case BOOLEAN -> ExpressionCreator.bool(nullable,
                        requireValue(literal, Boolean.class));
                case FLOAT, REAL -> ExpressionCreator.fp32(nullable, finiteFloat(literal));
                case DOUBLE -> ExpressionCreator.fp64(nullable, finiteDouble(literal));
                case DECIMAL -> convertDecimalLiteral(literal, fieldType);
                case DATE -> convertDateLiteral(literal, nullable);
                case TIME -> convertTimeLiteral(literal, fieldType);
                case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
                        convertTimestampLiteral(literal, fieldType);
                case INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND,
                        INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND,
                        INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND ->
                        convertIntervalDayLiteral(literal, nullable);
                case BINARY, VARBINARY -> convertBinaryLiteral(literal, nullable);
                case UUID -> ExpressionCreator.uuid(nullable, requireValue(literal, java.util.UUID.class));
                case NULL -> throw unsupported("non-NULL value with NULL type");
                default -> throw unsupported("literal type: " + fieldType.getSqlTypeName());
            };
        } catch (ArithmeticException | IllegalArgumentException | AssertionError failure) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Literal cannot be represented as " + fieldType.getSqlTypeName(),
                    failure);
        }
    }

    private static <T> T requireValue(RexLiteral literal, Class<T> valueType) {
        T value = literal.getValueAs(valueType);
        if (value == null) {
            throw new IllegalArgumentException("literal is not " + valueType.getSimpleName());
        }
        return value;
    }

    private static BigDecimal numeric(RexLiteral literal) {
        if (isFloatingLiteral(literal)) {
            // Use the stored binary value, not a rounded decimal representation.
            return new BigDecimal(finiteDouble(literal));
        }
        return requireValue(literal, BigDecimal.class);
    }

    /** Reads an integral literal without Calcite's truncating or unscaled long conversion. */
    static long exactLong(RexNode node) {
        if (!(node instanceof RexLiteral literal)) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_UNSUPPORTED_PLAN_NODE,
                    "Expected a literal BIGINT offset, not " + node.getClass().getSimpleName());
        }
        if (literal.isNull()) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Offset must not be NULL");
        }
        try {
            return numeric(literal).longValueExact();
        } catch (ArithmeticException | IllegalArgumentException | AssertionError failure) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Offset must be an exact BIGINT value", failure);
        }
    }

    private static double finiteDouble(RexLiteral literal) {
        double value = isFloatingLiteral(literal)
                ? requireValue(literal, Double.class)
                : numeric(literal).doubleValue();
        if (!Double.isFinite(value)) {
            throw new ArithmeticException("floating-point literal overflow");
        }
        return value;
    }

    private static float finiteFloat(RexLiteral literal) {
        float value = isFloatingLiteral(literal)
                ? requireValue(literal, Double.class).floatValue()
                : numeric(literal).floatValue();
        if (!Float.isFinite(value)) {
            throw new ArithmeticException("floating-point literal overflow");
        }
        return value;
    }

    private static boolean isFloatingLiteral(RexLiteral literal) {
        return literal.getTypeName() == SqlTypeName.FLOAT
                || literal.getTypeName() == SqlTypeName.REAL
                || literal.getTypeName() == SqlTypeName.DOUBLE;
    }

    private Expression convertDecimalLiteral(RexLiteral literal, RelDataType fieldType) {
        BigDecimal value = numeric(literal);
        var decimalType = (Type.Decimal) typeMapper.map(fieldType);
        int precision = decimalType.precision();
        int scale = decimalType.scale();
        value = value.setScale(scale, RoundingMode.UNNECESSARY);
        if (value.precision() > precision) {
            throw new ArithmeticException("decimal literal exceeds declared precision");
        }
        return ExpressionCreator.decimal(fieldType.isNullable(), value, precision, scale);
    }

    private Expression convertDateLiteral(RexLiteral literal, boolean nullable) {
        // Calcite stores DATE as days since epoch
        int daysSinceEpoch = requireValue(literal, Integer.class);
        return ExpressionCreator.date(nullable, daysSinceEpoch);
    }

    private Expression convertIntervalDayLiteral(RexLiteral literal, boolean nullable) {
        // Calcite stores day-time intervals as exact milliseconds. Substrait
        // carries separate days, seconds and fractional seconds at precision 6.
        long millis = numeric(literal).longValueExact();
        int days = Math.toIntExact(Math.floorDiv(millis, 86_400_000L));
        long withinDay = Math.floorMod(millis, 86_400_000L);
        int seconds = Math.toIntExact(withinDay / 1_000L);
        long micros = (withinDay % 1_000L) * 1_000L;
        return ExpressionCreator.intervalDay(nullable, days, seconds, micros, 6);
    }

    private Expression convertTimeLiteral(RexLiteral literal, RelDataType fieldType) {
        TimeString time = requireValue(literal, TimeString.class);
        int precision = temporalPrecision(fieldType);
        long seconds = time.getMillisOfDay() / 1000L;
        String text = time.toString(precision);
        BigDecimal fraction = precision == 0 ? BigDecimal.ZERO
                : new BigDecimal("0." + text.substring(9));
        long value = BigDecimal.valueOf(seconds).add(fraction)
                .scaleByPowerOfTen(precision).longValueExact();
        return ExpressionCreator.precisionTime(fieldType.isNullable(), value, precision);
    }

    private Expression convertTimestampLiteral(RexLiteral literal, RelDataType fieldType) {
        TimestampString timestamp = requireValue(literal, TimestampString.class);
        int precision = temporalPrecision(fieldType);
        // Precision literals count 10^precision units per second. Reading Calcite
        // as Long would discard fractions smaller than a millisecond.
        long seconds = Math.floorDiv(timestamp.getMillisSinceEpoch(), 1000L);
        String text = timestamp.toString(precision);
        BigDecimal fraction = precision == 0 ? BigDecimal.ZERO
                : new BigDecimal("0." + text.substring(20));
        long value = BigDecimal.valueOf(seconds).add(fraction)
                .scaleByPowerOfTen(precision).longValueExact();
        return fieldType.getSqlTypeName()
                == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                ? ExpressionCreator.precisionTimestampTZ(fieldType.isNullable(), value, precision)
                : ExpressionCreator.precisionTimestamp(fieldType.isNullable(), value, precision);
    }

    private static int temporalPrecision(RelDataType fieldType) {
        int precision = fieldType.getPrecision();
        return precision < 0 || precision == Integer.MAX_VALUE ? 6 : Math.min(precision, 9);
    }

    private Expression convertBinaryLiteral(RexLiteral literal, boolean nullable) {
        byte[] bytes = requireValue(literal, byte[].class);
        return ExpressionCreator.binary(nullable, bytes);
    }

    private static SubstraitConversionException unsupported(String detail) {
        return new SubstraitConversionException(
                PangrovaErrorCode.SUBSTRAIT_UNSUPPORTED_PLAN_NODE,
                "Unsupported " + detail);
    }
}
