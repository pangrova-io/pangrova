package org.pangrova.planner.validation;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/** Numeric arrays bind to the Native REAL or DOUBLE vector-distance signature at validation time. */
final class VectorDistanceSqlFunction extends SqlFunction {
    VectorDistanceSqlFunction(String name) {
        super(name, SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE_NULLABLE, null,
                OperandTypes.ARRAY_ARRAY, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        if (binding.getOperandCount() != 2) {
            return super.checkOperandTypes(binding, throwOnFailure);
        }
        RelDataType left = binding.getOperandType(0);
        RelDataType right = binding.getOperandType(1);
        if (textLiteral(binding, 1)) {
            // Query-text shorthand: the planner rewrites this literal to AI_EMBED(...,'QUERY').
            if (!numericArrayOrNull(left)) {
                if (throwOnFailure) throw binding.newValidationSignatureError();
                return false;
            }
            castArrayOperand(binding, 0, floatingType(left, right));
            return true;
        }
        if (!numericArrayOrNull(left) || !numericArrayOrNull(right)) {
            if (throwOnFailure) throw binding.newValidationSignatureError();
            return false;
        }
        SqlTypeName element = floatingType(left, right);
        castArrayOperand(binding, 0, element);
        castArrayOperand(binding, 1, element);
        return true;
    }

    private static boolean textLiteral(SqlCallBinding binding, int index) {
        RelDataType type = binding.getOperandType(index);
        return SqlTypeUtil.inCharFamily(type)
                && binding.isOperandLiteral(index, true)
                && !binding.isOperandNull(index, true);
    }

    private static void castArrayOperand(SqlCallBinding binding, int index, SqlTypeName element) {
        RelDataType original = binding.getOperandType(index);
        RelDataType component = original.getComponentType();
        var factory = binding.getTypeFactory();
        var scalar = factory.createTypeWithNullability(factory.createSqlType(element),
                component == null || component.isNullable());
        var target = factory.createTypeWithNullability(factory.createArrayType(scalar, -1), original.isNullable());
        SqlOperandCasts.cast(binding, index, target);
    }

    private static boolean numericArrayOrNull(RelDataType type) {
        if (type.getSqlTypeName() == SqlTypeName.NULL) return true;
        var component = type.getComponentType();
        return type.getSqlTypeName() == SqlTypeName.ARRAY && component != null
                && (SqlTypeUtil.isNumeric(component) || component.getSqlTypeName() == SqlTypeName.NULL);
    }

    private static SqlTypeName floatingType(RelDataType left, RelDataType right) {
        var first = left.getComponentType();
        var second = right.getComponentType();
        if (is(first, SqlTypeName.DOUBLE) || is(second, SqlTypeName.DOUBLE)) return SqlTypeName.DOUBLE;
        if (isReal(first) || isReal(second)) return SqlTypeName.REAL;
        return SqlTypeName.DOUBLE;
    }

    private static boolean isReal(RelDataType type) {
        return is(type, SqlTypeName.REAL) || is(type, SqlTypeName.FLOAT);
    }

    private static boolean is(RelDataType type, SqlTypeName name) {
        return type != null && type.getSqlTypeName() == name;
    }
}
