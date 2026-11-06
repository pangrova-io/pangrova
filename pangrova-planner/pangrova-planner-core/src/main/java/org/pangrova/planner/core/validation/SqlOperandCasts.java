package org.pangrova.planner.core.validation;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;

/** Writes signature-required casts into validated SQL so Rex and Substrait share the same contract. */
final class SqlOperandCasts {
    private SqlOperandCasts() {}

    static void cast(SqlCallBinding binding, int index, RelDataType target) {
        if (binding.getOperandType(index).equals(target)) return;
        var operand = binding.operand(index);
        var cast = SqlStdOperatorTable.CAST.createCall(operand.getParserPosition(), operand,
                SqlTypeUtil.convertTypeToSpec(target));
        binding.getValidator().deriveType(binding.getScope(), cast);
        binding.getCall().setOperand(index, cast);
    }
}
