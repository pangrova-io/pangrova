package org.pangrova.planner.core.validation;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/** ROUND's decimal return parameters and optional INTEGER position match the Native signature. */
final class RoundSqlFunction extends SqlFunction {
    RoundSqlFunction() {
        super("ROUND", SqlKind.OTHER_FUNCTION, RoundSqlFunction::resultType, null,
                OperandTypes.NUMERIC_OPTIONAL_INTEGER, SqlFunctionCategory.NUMERIC);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        if (!super.checkOperandTypes(binding, throwOnFailure)) return false;
        if (binding.getOperandCount() == 2) {
            var factory = binding.getTypeFactory();
            var integer = factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.INTEGER),
                    binding.getOperandType(1).isNullable());
            SqlOperandCasts.cast(binding, 1, integer);
        }
        return true;
    }

    private static RelDataType resultType(SqlOperatorBinding binding) {
        RelDataType input = binding.getOperandType(0);
        var factory = binding.getTypeFactory();
        RelDataType result = input;
        if (input.getSqlTypeName() == SqlTypeName.DECIMAL) {
            int scale = binding.getOperandCount() == 1 ? 0 : input.getScale();
            int precision = binding.getOperandCount() == 1
                    ? input.getPrecision() - input.getScale() + Math.min(input.getScale(), 1)
                    : input.getPrecision() + 1;
            result = factory.createSqlType(SqlTypeName.DECIMAL,
                    Math.min(factory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL), precision), scale);
        }
        boolean nullable = false;
        for (int index = 0; index < binding.getOperandCount(); index++) {
            nullable |= binding.getOperandType(index).isNullable();
        }
        return factory.createTypeWithNullability(result, nullable);
    }
}
