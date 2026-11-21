package org.pangrova.planner.substrait;

import org.pangrova.planner.exception.SubstraitConversionException;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WindowBoundLiteralContractTest {
    @Test
    void nullOffsetFailsExplicitlyRatherThanUnboxingOrUsingZero() {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var nullOffset = rex.makeNullLiteral(types.createSqlType(SqlTypeName.BIGINT));
        assertThatThrownBy(() -> WindowFunctionConverter.extractLong(nullOffset))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("must not be NULL");
    }

    @Test
    void boundedWindowRejectsNegativeButAcceptsZeroAndPositiveOffsets() {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var typeMapper = new TypeMapper();
        var converter = new WindowFunctionConverter(typeMapper, new FunctionMapper(),
                new LiteralConverter(typeMapper), rex);
        assertThatThrownBy(() -> converter.convertWindowBound(
                RexWindowBounds.preceding(rex.makeExactLiteral(BigDecimal.valueOf(-1)))))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("non-negative");
        assertThat(converter.convertWindowBound(RexWindowBounds.preceding(rex.makeExactLiteral(BigDecimal.ZERO))))
                .isEqualTo(io.substrait.expression.WindowBound.CURRENT_ROW);
        assertThat(converter.convertWindowBound(RexWindowBounds.following(rex.makeExactLiteral(BigDecimal.ZERO))))
                .isEqualTo(io.substrait.expression.WindowBound.CURRENT_ROW);
        assertThat(WindowFunctionConverter.extractLong(rex.makeExactLiteral(BigDecimal.valueOf(7))))
                .isEqualTo(7);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.5", "9223372036854775808", "-9223372036854775809"})
    void boundedWindowRejectsFractionAndOverflowWithoutTruncating(String value) {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var typeMapper = new TypeMapper();
        var converter = new WindowFunctionConverter(typeMapper, new FunctionMapper(),
                new LiteralConverter(typeMapper), rex);
        var offset = rex.makeExactLiteral(new BigDecimal(value),
                types.createSqlType(SqlTypeName.DECIMAL, 38, value.contains(".") ? 1 : 0));

        assertThatThrownBy(() -> converter.convertWindowBound(RexWindowBounds.preceding(offset)))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("exact BIGINT");
    }

    @Test
    void decimalBoundReadsNumericValueRatherThanUnscaledDigits() {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var offset = rex.makeExactLiteral(new BigDecimal("2.0"),
                types.createSqlType(SqlTypeName.DECIMAL, 2, 1));

        assertThat(WindowFunctionConverter.extractLong(offset)).isEqualTo(2L);
    }

    @Test
    void boundedWindowRejectsDynamicOffsetInsteadOfChoosingDefault() {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var typeMapper = new TypeMapper();
        var converter = new WindowFunctionConverter(typeMapper, new FunctionMapper(),
                new LiteralConverter(typeMapper), rex);
        var offset = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 0);

        assertThatThrownBy(() -> converter.convertWindowBound(RexWindowBounds.preceding(offset)))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("literal BIGINT");
    }
}
