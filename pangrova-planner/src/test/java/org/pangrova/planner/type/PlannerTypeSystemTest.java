package org.pangrova.planner.type;

import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.planner.PangrovaPlanner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlannerTypeSystemTest {

    @Test
    void singlePrecisionFloatCannotHideDoubleInEitherOperandOrder() {
        var foreign = new org.apache.calcite.jdbc.JavaTypeFactoryImpl().createSqlType(SqlTypeName.FLOAT);
        assertThat(foreign.getPrecision()).isEqualTo(15);
        var factory = new PlannerTypeFactory();
        var single = factory.createSqlType(SqlTypeName.FLOAT);
        var wide = factory.createSqlType(SqlTypeName.DOUBLE);
        assertThat(single.getPrecision()).isEqualTo(factory.createSqlType(SqlTypeName.REAL).getPrecision());
        assertThat(single.getPrecision()).isLessThan(wide.getPrecision());
        assertThat(factory.leastRestrictive(List.of(single, wide)).getSqlTypeName()).isEqualTo(SqlTypeName.DOUBLE);
        assertThat(factory.leastRestrictive(List.of(wide, single)).getSqlTypeName()).isEqualTo(SqlTypeName.DOUBLE);
    }

    @Test
    void productionFactoryPreservesEverySupportedDecimalPrecisionAndScale() {
        var factory = new PangrovaPlanner().plan("SELECT 1").plan().getCluster().getTypeFactory();
        assertThat(factory).isInstanceOf(PlannerTypeFactory.class);
        assertThat(factory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL)).isEqualTo(38);
        assertThat(factory.getTypeSystem().getMaxScale(SqlTypeName.DECIMAL)).isEqualTo(38);
        assertThat(factory.createSqlType(SqlTypeName.DECIMAL).getPrecision()).isEqualTo(38);
        assertThat(factory.createSqlType(SqlTypeName.DECIMAL).getScale()).isZero();
        for (int precision = 1; precision <= 38; precision++) {
            for (int scale = 0; scale <= precision; scale++) {
                var decimal = factory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
                assertThat(decimal.getPrecision()).isEqualTo(precision);
                assertThat(decimal.getScale()).isEqualTo(scale);
                assertThat(factory.createTypeWithNullability(decimal, true).isNullable()).isTrue();
            }
        }
    }

    @Test
    void canonicalFactoryRejectsInvalidDecimalInsteadOfClamping() {
        var factory = new PlannerTypeFactory();
        for (int precision : new int[] {-1, 0, 39, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> factory.createSqlType(SqlTypeName.DECIMAL, precision, 0))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("DECIMAL precision");
            assertThatThrownBy(() -> factory.createSqlType(SqlTypeName.DECIMAL, precision))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("DECIMAL precision");
        }
        for (int scale : new int[] {-1, 11, 39, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> factory.createSqlType(SqlTypeName.DECIMAL, 10, scale))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("DECIMAL scale");
        }
    }

    @Test
    void productionFactoryPreservesSupportedTemporalPrecisions() {
        var factory = new PangrovaPlanner().plan("SELECT 1").plan()
                .getCluster().getTypeFactory();

        for (var type : new SqlTypeName[] {
                SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                SqlTypeName.TIMESTAMP_TZ}) {
            assertThat(factory.createSqlType(type).getPrecision()).isEqualTo(6);
            assertThat(factory.createSqlType(type, 9).getPrecision()).isEqualTo(9);
        }
        assertThat(factory.createSqlType(SqlTypeName.TIME).getPrecision()).isEqualTo(6);
        assertThat(factory.createSqlType(SqlTypeName.TIME, 6).getPrecision()).isEqualTo(6);
    }
}
