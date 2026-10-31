package org.pangrova.parser.ast;

/**
 * Represents a literal value (integer, decimal, string, boolean, null, date, timestamp).
 */
public record Literal(LiteralValue value, LiteralType literalType) implements Expression {

    public enum LiteralType {
        INTEGER,
        DECIMAL,
        STRING,
        BOOLEAN,
        NULL,
        DATE,
        TIMESTAMP,
        TIMESTAMP_WITH_TIME_ZONE,
        INTERVAL,
        BINARY_HEX,
        UUID
    }

    public static Literal ofInteger(long value) {
        return new Literal(new LiteralValue.IntegerValue(value), LiteralType.INTEGER);
    }

    public static Literal ofDecimal(java.math.BigDecimal value) {
        return new Literal(new LiteralValue.DecimalValue(value), LiteralType.DECIMAL);
    }

    public static Literal ofString(String value) {
        return new Literal(new LiteralValue.StringValue(value), LiteralType.STRING);
    }

    public static Literal ofBoolean(boolean value) {
        return new Literal(new LiteralValue.BooleanValue(value), LiteralType.BOOLEAN);
    }

    public static Literal ofUuid(java.util.UUID value) {
        return new Literal(new LiteralValue.UuidValue(value), LiteralType.UUID);
    }

    public static Literal ofNull() {
        return new Literal(new LiteralValue.NullValue(), LiteralType.NULL);
    }

    public static Literal ofDate(String value) {
        return new Literal(new LiteralValue.DateValue(value), LiteralType.DATE);
    }

    public static Literal ofTimestamp(String value) {
        return new Literal(new LiteralValue.TimestampValue(value), LiteralType.TIMESTAMP);
    }

    public static Literal ofInterval(String expression, String fromUnit, String toUnit) {
        return new Literal(new LiteralValue.IntervalValue(expression, fromUnit, toUnit), LiteralType.INTERVAL);
    }

    public static Literal ofInterval(String expression, String unit) {
        return new Literal(new LiteralValue.IntervalValue(expression, unit), LiteralType.INTERVAL);
    }

    public static Literal ofBinaryHex(String hexDigits) {
        return new Literal(new LiteralValue.BinaryHexValue(hexDigits), LiteralType.BINARY_HEX);
    }

    public static Literal ofTimestampWithTimeZone(String value) {
        return new Literal(new LiteralValue.TimestampWithTimeZoneValue(value), LiteralType.TIMESTAMP_WITH_TIME_ZONE);
    }

    /** Returns true if this literal represents SQL NULL. */
    public boolean isNull() {
        return value instanceof LiteralValue.NullValue;
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.LITERAL;
    }
}
