package org.pangrova.planner.substrait;

import org.pangrova.planner.exception.SubstraitConversionException;

import io.substrait.expression.ExpressionCreator;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.planner.type.PlannerTypeFactory;
import org.pangrova.planner.type.PlannerTypeSystem;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LiteralConverterNumericTest {

    private final PlannerTypeFactory types = new PlannerTypeFactory();
    private final RexBuilder rex = new RexBuilder(types);
    private final LiteralConverter literals = new LiteralConverter(new TypeMapper());

    @ParameterizedTest
    @CsvSource({"0,0", "-2147483648,2", "39,2", "10,-2", "10,11"})
    void malformedDecimalContextIsRejectedRatherThanDerivedFromItsValue(int precision, int scale) {
        var malformed = new BasicSqlType(PlannerTypeSystem.INSTANCE, SqlTypeName.DECIMAL, precision, scale);
        var literal = rex.makeExactLiteral(new BigDecimal("1.23"));
        assertThatThrownBy(() -> literals.convert(literal, malformed))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("Invalid DECIMAL");
        assertThatThrownBy(() -> literals.convert(rex.makeNullLiteral(malformed)))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("Invalid DECIMAL");
    }

    @Test
    void omittedScaleSentinelUsesZeroOnlyForARepresentableLiteral() {
        var type = new BasicSqlType(PlannerTypeSystem.INSTANCE, SqlTypeName.DECIMAL, 10,
                org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED);
        assertThat(literals.convert(rex.makeExactLiteral(new BigDecimal("1.00")), type))
                .isEqualTo(ExpressionCreator.decimal(false, BigDecimal.ONE, 10, 0));
        assertThatThrownBy(() -> literals.convert(rex.makeExactLiteral(new BigDecimal("1.23")), type))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("cannot be represented");
    }

    @ParameterizedTest
    @CsvSource({"39,2", "10,11"})
    void malformedRexLiteralTypeIsRejectedDirectly(int precision, int scale) {
        var malformed = new BasicSqlType(PlannerTypeSystem.INSTANCE, SqlTypeName.DECIMAL, precision, scale);
        var literal = rex.makeExactLiteral(BigDecimal.ZERO, malformed);
        assertThatThrownBy(() -> literals.convert(literal))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("Invalid DECIMAL");
    }

    @ParameterizedTest
    @ValueSource(strings = {"12345678901234567890.123456789012345678",
            "-12345678901234567890.123456789012345678"})
    void decimal38LiteralPreservesAllDigitsWithoutFallback(String text) {
        var value = new BigDecimal(text);
        var type = types.createSqlType(SqlTypeName.DECIMAL, 38, 18);
        assertThat(literals.convert(rex.makeExactLiteral(value, type)))
                .isEqualTo(ExpressionCreator.decimal(false, value, 38, 18));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.234", "123456789012345678901.00"})
    void decimalConversionRejectsScaleLossAndValueOverflow(String text) {
        var value = new BigDecimal(text);
        var source = types.createSqlType(SqlTypeName.DECIMAL, 38, value.scale());
        var target = types.createSqlType(SqlTypeName.DECIMAL, text.equals("1.234") ? 10 : 20, 2);
        assertThatThrownBy(() -> literals.convert(rex.makeExactLiteral(value, source), target))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("cannot be represented");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-2", "2.0", "9223372036854775807", "-9223372036854775808"})
    void readsTheExactNumericValueRatherThanUnscaledOrWrappedDigits(String text) {
        var value = new BigDecimal(text);
        var literal = rex.makeExactLiteral(value);

        assertThat(LiteralConverter.exactLong(literal)).isEqualTo(value.longValueExact());
        assertThat(literals.convert(literal, types.createSqlType(SqlTypeName.BIGINT)))
                .isEqualTo(ExpressionCreator.i64(false, value.longValueExact()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.5", "9223372036854775808", "-9223372036854775809"})
    void exactLongRejectsFractionAndOverflow(String text) {
        var literal = rex.makeExactLiteral(new BigDecimal(text));

        assertThatThrownBy(() -> LiteralConverter.exactLong(literal))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("exact BIGINT");
    }

    @Test
    void floatingPointIntegralValueDoesNotPassThroughDecimal64Rounding() {
        long expected = 1_234_567_890_123_456_768L;
        var literal = rex.makeApproxLiteral((double) expected, types.createSqlType(SqlTypeName.DOUBLE));

        assertThat(LiteralConverter.exactLong(literal)).isEqualTo(expected);
        assertThat(literals.convert(literal, types.createSqlType(SqlTypeName.BIGINT)))
                .isEqualTo(ExpressionCreator.i64(false, expected));
    }

    @Test
    void floatingPointLongOverflowIsRejectedRatherThanSaturated() {
        var literal = rex.makeApproxLiteral((double) Long.MAX_VALUE, types.createSqlType(SqlTypeName.DOUBLE));

        assertThatThrownBy(() -> LiteralConverter.exactLong(literal))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("exact BIGINT");
    }

    @Test
    void nonNumericLiteralFailsExplicitly() {
        assertThatThrownBy(() -> LiteralConverter.exactLong(rex.makeLiteral(true)))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("exact BIGINT");
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1.5})
    void nonFiniteOrFractionalFloatingOffsetFailsExplicitly(double value) {
        var literal = rex.makeApproxLiteral(value, types.createSqlType(SqlTypeName.DOUBLE));

        assertThatThrownBy(() -> LiteralConverter.exactLong(literal))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("exact BIGINT");
    }
}
