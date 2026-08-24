package org.pangrova.parser.ast;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Type-safe representation of SQL literal values.
 * Replaces raw Object in Literal record to enforce compile-time type safety.
 */
public sealed interface LiteralValue {

    record IntegerValue(long value) implements LiteralValue {}

    record DecimalValue(BigDecimal value) implements LiteralValue {
        public DecimalValue {
            Objects.requireNonNull(value, "decimal literal value must not be null");
        }
    }

    record StringValue(String value) implements LiteralValue {
        public StringValue {
            Objects.requireNonNull(value, "string literal value must not be null");
        }
    }

    record BooleanValue(boolean value) implements LiteralValue {}

    record NullValue() implements LiteralValue {}

    record DateValue(String value) implements LiteralValue {
        public DateValue {
            Objects.requireNonNull(value, "date literal value must not be null");
        }
    }

    record TimestampValue(String value) implements LiteralValue {
        public TimestampValue {
            Objects.requireNonNull(value, "timestamp literal value must not be null");
        }
    }

    record IntervalValue(String expression, String fromUnit, String toUnit) implements LiteralValue {
        public IntervalValue {
            Objects.requireNonNull(expression, "interval expression must not be null");
            Objects.requireNonNull(fromUnit, "interval from-unit must not be null");
        }

        /** Single-unit interval (e.g., INTERVAL '1' DAY). */
        public IntervalValue(String expression, String unit) {
            this(expression, unit, null);
        }

        /** Returns true if this is a range interval (e.g., YEAR TO MONTH). */
        public boolean isRange() {
            return toUnit != null;
        }
    }

    record BinaryHexValue(String hexDigits) implements LiteralValue {
        public BinaryHexValue {
            Objects.requireNonNull(hexDigits, "hex literal value must not be null");
        }
    }

    record TimestampWithTimeZoneValue(String value) implements LiteralValue {
        public TimestampWithTimeZoneValue {
            Objects.requireNonNull(value, "timestamp with time zone value must not be null");
        }
    }
}
