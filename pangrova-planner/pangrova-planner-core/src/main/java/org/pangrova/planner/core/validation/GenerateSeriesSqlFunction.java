package org.pangrova.planner.core.validation;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/** Relational GENERATE_SERIES binding; rows are produced only by the Native operator. */
final class GenerateSeriesSqlFunction extends SqlFunction implements SqlTableFunction {
    private static final SqlOperandTypeChecker ARGUMENTS = OperandTypes.or(
            OperandTypes.INTEGER_INTEGER,
            OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER));

    GenerateSeriesSqlFunction() {
        super("GENERATE_SERIES", SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, null,
                ARGUMENTS, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return binding -> binding.getTypeFactory().builder()
                .add("val", SqlTypeName.BIGINT).build();
    }
}
