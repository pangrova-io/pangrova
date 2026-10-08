package org.pangrova.planner.validation;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.pangrova.spi.function.FunctionMetadata;

/** Projects the existing callback declaration's execution stability into SQL optimization. */
class DeclaredCallbackSqlFunction extends SqlFunction {
    private final boolean deterministic;

    DeclaredCallbackSqlFunction(String name, SqlReturnTypeInference returnType,
                               SqlOperandTypeChecker operands, FunctionMetadata metadata) {
        super(name, SqlKind.OTHER_FUNCTION, returnType, null, operands,
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
