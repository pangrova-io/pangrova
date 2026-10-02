package org.pangrova.planner.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlannerDecimalArithmeticTest {

    private final PlannerTypeFactory factory = new PlannerTypeFactory();
    private final PlannerTypeSystem system = PlannerTypeSystem.INSTANCE;

    @ParameterizedTest
    @CsvSource({
            "+,10,2,20,3,21,3", "-,10,2,20,3,21,3",
            "*,10,2,20,3,30,5", "/,10,2,20,3,14,3", "%,10,2,20,3,11,3",
            "+,38,38,38,38,38,38", "*,38,18,38,18,38,36",
            "/,38,18,38,18,38,18", "/,20,1,38,19,38,19",
            "%,10,0,20,0,10,0", "%,20,0,10,0,10,0", "%,38,38,38,38,38,38"
    })
    void arithmeticUsesNativeDecimalSignature(String operator, int leftPrecision, int leftScale,
            int rightPrecision, int rightScale, int resultPrecision, int resultScale) {
        var result = derive(operator, decimal(leftPrecision, leftScale), decimal(rightPrecision, rightScale));
        assertThat(result.getSqlTypeName()).isEqualTo(SqlTypeName.DECIMAL);
        assertThat(result.getPrecision()).isEqualTo(resultPrecision);
        assertThat(result.getScale()).isEqualTo(resultScale);
    }

    @Test
    void rejectsMultiplicationWhoseScaleCannotBeRepresented() {
        assertThatThrownBy(() -> derive("*", decimal(38, 20), decimal(38, 19)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("scale exceeds 38");
    }

    @Test
    void rejectsDivisionWhoseNativeRescaleCannotBeRepresented() {
        assertThatThrownBy(() -> derive("/", decimal(38, 0), decimal(38, 20)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rescale exceeds 38");
    }

    @Test
    void supportsSqlAndJavaIntegerOperandsWithoutChangingIntegerOnlyArithmetic() {
        var integer = factory.createSqlType(SqlTypeName.INTEGER);
        assertThat(derive("*", decimal(20, 2), integer)).isEqualTo(decimal(30, 2));
        assertThat(derive("/", decimal(20, 2), integer)).isEqualTo(decimal(20, 2));
        assertThat(derive("*", decimal(20, 2), factory.createJavaType(Integer.class)))
                .isEqualTo(decimal(30, 2));
        for (String operator : new String[] {"*", "/", "%"}) {
            assertThat(derive(operator, integer, integer)).isNull();
            assertThat(derive(operator, decimal(10, 2), factory.createSqlType(SqlTypeName.DOUBLE))).isNull();
            assertThat(derive(operator, factory.createSqlType(SqlTypeName.VARCHAR), decimal(10, 2))).isNull();
        }
    }

    @Test
    void decimalSumWidensToTheSameMaximumWithoutChangingScale() {
        assertThat(system.deriveSumType(factory, decimal(20, 18))).isEqualTo(decimal(38, 18));
    }

    private RelDataType decimal(int precision, int scale) {
        return factory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
    }

    private RelDataType derive(String operator, RelDataType left, RelDataType right) {
        return switch (operator) {
            case "+", "-" -> system.deriveDecimalPlusType(factory, left, right);
            case "*" -> system.deriveDecimalMultiplyType(factory, left, right);
            case "/" -> system.deriveDecimalDivideType(factory, left, right);
            case "%" -> system.deriveDecimalModType(factory, left, right);
            default -> throw new IllegalArgumentException(operator);
        };
    }
}
