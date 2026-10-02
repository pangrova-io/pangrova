package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import org.apache.calcite.rel.type.RelDataType;
import org.pangrova.planner.type.PlannerTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimeString;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LiteralConverterTimestampTest {

    private final SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(PlannerTypeSystem.INSTANCE);
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final LiteralConverter converter = new LiteralConverter(new TypeMapper());

    @ParameterizedTest
    @MethodSource("timestamps")
    void encodesTimestampInItsDeclaredUnits(
            String text, int precision, long expectedValue, boolean withTimezone) {
        RexLiteral literal = timestamp(text, precision, withTimezone);
        Expression expected = withTimezone
                ? ExpressionCreator.precisionTimestampTZ(false, expectedValue, precision)
                : ExpressionCreator.precisionTimestamp(false, expectedValue, precision);

        assertThat(converter.convert(literal)).isEqualTo(expected);
    }

    @Test
    void preservesFractionWhenValuesFieldWidensPrecisionAndNullability() {
        RexLiteral literal = timestamp("2025-01-01 00:00:00.123456", 6, false);
        RelDataType fieldType = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 9), true);

        assertThat(converter.convert(literal, fieldType)).isEqualTo(
                ExpressionCreator.precisionTimestamp(true, 1735689600123456000L, 9));
    }

    @Test
    void rejectsUnrepresentableNanosecondEpochWithoutWrapping() {
        RexLiteral literal = timestamp("2300-01-01 00:00:00.000000001", 9, false);

        assertThatThrownBy(() -> converter.convert(literal))
                .isInstanceOf(SubstraitConversionException.class)
                .hasMessageContaining("cannot be represented")
                .hasCauseInstanceOf(ArithmeticException.class);
    }

    @ParameterizedTest
    @CsvSource({"00:00:00, 0, 0", "12:34:56, 0, 45296",
            "12:34:56.123, 3, 45296123", "23:59:59.999999, 6, 86399999999"})
    void timeLiteralUsesItsDeclaredUnits(String text, int precision, long expectedValue) {
        RexLiteral literal = rexBuilder.makeTimeLiteral(new TimeString(text), precision);

        assertThat(converter.convert(literal)).isEqualTo(
                ExpressionCreator.precisionTime(false, expectedValue, precision));
    }

    private RexLiteral timestamp(String text, int precision, boolean withTimezone) {
        TimestampString value = new TimestampString(text);
        return withTimezone
                ? rexBuilder.makeTimestampWithLocalTimeZoneLiteral(value, precision)
                : rexBuilder.makeTimestampLiteral(value, precision);
    }

    private static Stream<Arguments> timestamps() {
        Object[][] values = {
                {"1970-01-01 00:00:00", 0, 0L},
                {"2025-01-01 00:00:00", 0, 1735689600L},
                {"2025-01-01 00:00:00.123", 3, 1735689600123L},
                {"2025-01-01 00:00:00.123456", 6, 1735689600123456L},
                {"2025-01-01 00:00:00.123456789", 9, 1735689600123456789L},
                {"1969-12-31 23:59:59", 0, -1L},
                {"1969-12-31 23:59:59.999", 3, -1L},
                {"1969-12-31 23:59:59.999999", 6, -1L},
                {"1969-12-31 23:59:59.999999999", 9, -1L},
                {"1677-09-21 00:12:43.145224192", 9, Long.MIN_VALUE},
                {"2262-04-11 23:47:16.854775807", 9, Long.MAX_VALUE}
        };
        return Stream.of(values).flatMap(value -> Stream.of(false, true)
                .map(withTimezone -> Arguments.of(value[0], value[1], value[2], withTimezone)));
    }
}
