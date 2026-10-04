package org.pangrova.planner.validation;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import static org.apache.calcite.util.Static.RESOURCE;

/** The optional enum constraint and default come from the canonical declaration. */
final class NormalizedAiEmbeddingSqlFunction extends SqlFunction {
    NormalizedAiEmbeddingSqlFunction(SqlReturnTypeInference returnType) {
        super("AI_EMBED", SqlKind.OTHER_FUNCTION, returnType, null,
                operands(), SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        var argument = FunctionBindingDeclarations.normalizedArguments("SCALAR", "ai", "ai_embed").get(0);
        if (binding.getOperandCount() <= argument.getOrdinal()) {
            return super.checkOperandTypes(binding, throwOnFailure);
        }
        int ordinal = argument.getOrdinal();
        if (!SqlTypeUtil.inCharFamily(binding.getOperandType(ordinal))
                || !binding.isOperandLiteral(ordinal, true) || binding.isOperandNull(ordinal, true)) {
            return reject(binding, throwOnFailure);
        }
        String value = binding.getOperandLiteralValue(ordinal, String.class);
        int precision = binding.getOperandType(ordinal).getPrecision();
        if (value == null || (precision >= 0 && value.length() > precision)
                || !argument.getEnumValuesList().contains(value)) {
            return reject(binding, throwOnFailure);
        }
        return super.checkOperandTypes(binding, throwOnFailure);
    }

    private static boolean reject(SqlCallBinding binding, boolean throwOnFailure) {
        if (throwOnFailure) {
            var argument = FunctionBindingDeclarations.normalizedArguments("SCALAR", "ai", "ai_embed").get(0);
            throw binding.newError(RESOURCE.canNotApplyOp2Type("AI_EMBED purpose",
                    binding.getOperandType(argument.getOrdinal()).toString(),
                    "non-null constant " + argument.getEnumValuesList()));
        }
        return false;
    }

    private static SqlOperandTypeChecker operands() {
        var character = OperandTypes.CHARACTER;
        var file = OperandTypes.typeName(SqlTypeName.ROW);
        return OperandTypes.or(
                OperandTypes.sequence("(<CHARACTER>, <CHARACTER>)", character, character),
                OperandTypes.sequence("(<CHARACTER>, <ROW>)", character, file),
                OperandTypes.sequence("(<CHARACTER>, <CHARACTER>, <PURPOSE>)", character, character, character),
                OperandTypes.sequence("(<CHARACTER>, <ROW>, <PURPOSE>)", character, file, character));
    }
}
