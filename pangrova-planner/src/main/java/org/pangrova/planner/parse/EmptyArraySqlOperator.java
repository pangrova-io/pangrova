package org.pangrova.planner.parse;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.SqlCall;

/** Allows an empty array literal to acquire its element type from its SQL context. */
final class EmptyArraySqlOperator extends SqlArrayValueConstructor {

    static final EmptyArraySqlOperator INSTANCE = new EmptyArraySqlOperator();

    private EmptyArraySqlOperator() {}

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        // SqlOperator.deriveType resolves by name and replaces this instance with
        // Calcite's built-in ARRAY operator, which rejects zero operands.
        return validateOperands(validator, scope, call);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        return binding.getOperandCount() == 0
                || super.checkOperandTypes(binding, throwOnFailure);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding binding) {
        if (binding.getOperandCount() != 0) {
            return super.inferReturnType(binding);
        }
        var element = binding.getTypeFactory().createSqlType(SqlTypeName.NULL);
        return binding.getTypeFactory().createArrayType(element, -1);
    }
}
