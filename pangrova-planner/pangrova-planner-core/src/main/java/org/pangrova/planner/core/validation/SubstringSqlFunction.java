package org.pangrova.planner.core.validation;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/** Binds all substring positions to the same Native BIGINT signature. */
final class SubstringSqlFunction extends SqlFunction {
    SubstringSqlFunction() {
        super("SUBSTRING", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE_VARYING, null,
                OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER, SqlFunctionCategory.STRING);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        if (!super.checkOperandTypes(binding, throwOnFailure)) return false;
        for (int index = 1; index < binding.getOperandCount(); index++) {
            var factory = binding.getTypeFactory();
            var target = factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT),
                    binding.getOperandType(index).isNullable());
            SqlOperandCasts.cast(binding, index, target);
        }
        return true;
    }
}
