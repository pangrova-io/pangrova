package org.pangrova.planner.core.validation;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.pangrova.function.api.metadata.FunctionMetadata;

/** Projects the existing callback declaration's execution stability into SQL optimization. */
class DeclaredCallbackSqlFunction extends SqlFunction {
    private final boolean deterministic;

    DeclaredCallbackSqlFunction(String name, SqlReturnTypeInference returnType,
                               SqlOperandTypeChecker operands, FunctionMetadata metadata) {
        super(name, SqlKind.OTHER_FUNCTION, returnType.andThen(SqlTypeTransforms.FORCE_NULLABLE), null, operands,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
        deterministic = metadata.deterministic();
    }

    @Override
    public boolean isDeterministic() {
        return deterministic;
    }

    @Override
    public boolean isDynamicFunction() {
        return !deterministic;
    }
}
