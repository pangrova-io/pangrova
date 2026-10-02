package org.pangrova.planner.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/** Derives decimal arithmetic types from the exact operand signatures sent to Native. */
final class DecimalArithmeticTypeDerivation {

    private DecimalArithmeticTypeDerivation() {}

    static RelDataType multiply(RelDataTypeFactory factory, RelDataType left, RelDataType right) {
        Operands operands = operands(factory, left, right);
        if (operands == null) return null;
        return decimal(factory, operands.left().getPrecision() + operands.right().getPrecision(),
                operands.left().getScale() + operands.right().getScale());
    }

    static RelDataType divide(RelDataTypeFactory factory, RelDataType left, RelDataType right) {
        Operands operands = operands(factory, left, right);
        if (operands == null) return null;
        int leftScale = operands.left().getScale();
        int rightScale = operands.right().getScale();
        int scale = Math.max(leftScale, rightScale);
        int maximum = factory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL);
        if (scale - leftScale + rightScale > maximum) {
            throw new IllegalArgumentException("DECIMAL division rescale exceeds " + maximum);
        }
        int precision = operands.left().getPrecision() + rightScale + Math.max(0, rightScale - leftScale);
        return decimal(factory, precision, scale);
    }

    static RelDataType modulus(RelDataTypeFactory factory, RelDataType left, RelDataType right) {
        Operands operands = operands(factory, left, right);
        if (operands == null) return null;
        int scale = Math.max(operands.left().getScale(), operands.right().getScale());
        int integers = Math.min(operands.left().getPrecision() - operands.left().getScale(),
                operands.right().getPrecision() - operands.right().getScale());
        return decimal(factory, integers + scale, scale);
    }

    private static RelDataType decimal(RelDataTypeFactory factory, int precision, int scale) {
        int maximum = factory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL);
        if (scale > maximum) {
            throw new IllegalArgumentException("DECIMAL arithmetic scale exceeds " + maximum + ": " + scale);
        }
        return factory.createSqlType(SqlTypeName.DECIMAL, Math.min(precision, maximum), scale);
    }

    private static Operands operands(RelDataTypeFactory factory, RelDataType left, RelDataType right) {
        if (!SqlTypeUtil.isExactNumeric(left) || !SqlTypeUtil.isExactNumeric(right)) return null;
        if (!SqlTypeUtil.isDecimal(left) && !SqlTypeUtil.isDecimal(right)) return null;
        return new Operands(normalize(factory, left), normalize(factory, right));
    }

    private static RelDataType normalize(RelDataTypeFactory factory, RelDataType type) {
        return RelDataTypeFactoryImpl.isJavaType(type) ? factory.decimalOf(type) : type;
    }

    private record Operands(RelDataType left, RelDataType right) {}
}
