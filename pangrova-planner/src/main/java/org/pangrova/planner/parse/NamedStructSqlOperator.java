package org.pangrova.planner.parse;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.fun.SqlRowOperator;

/** Preserves explicitly declared constructor field names in Calcite's ordinary ROW type. */
final class NamedStructSqlOperator extends SqlRowOperator {
    private final List<String> fields;

    NamedStructSqlOperator(List<String> fields) {
        super("ROW");
        this.fields = List.copyOf(fields);
        var unique = new HashSet<String>();
        for (String field : fields) {
            if (!unique.add(field.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Duplicate struct field name: " + field);
            }
        }
    }

    @Override public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        // The field declaration belongs to this call, not the global positional ROW operator.
        for (var operand : call.getOperandList()) validator.deriveType(scope, operand);
        return validateOperands(validator, scope, call);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding binding) {
        if (binding.getOperandCount() != fields.size()) {
            throw new IllegalArgumentException("Struct field and operand counts differ");
        }
        var result = binding.getTypeFactory().builder();
        for (int i = 0; i < fields.size(); i++) result.add(fields.get(i), binding.getOperandType(i));
        return binding.getTypeFactory().createTypeWithNullability(
                result.build(), super.inferReturnType(binding).isNullable());
    }
}
