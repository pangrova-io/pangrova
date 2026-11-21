package org.pangrova.planner.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.protocol.function.declaration.DeclaredFunctionReference;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.v2.FunctionKind;

import java.util.List;

/** Preserves the declared Native UUID ordering through Calcite simplification. */
public final class UuidOrderingSqlFunction extends SqlFunction {
    private final SqlKind comparison;
    private final DeclaredFunctionReference declaration;

    private UuidOrderingSqlFunction(SqlKind comparison, DeclaredFunctionReference declaration) {
        super(declaration.name(), SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN_NULLABLE,
                null, OperandTypes.ANY_ANY, SqlFunctionCategory.SYSTEM);
        this.comparison = comparison;
        this.declaration = declaration;
    }

    /** Binds only UUID-containing ordered operands to their sole declared Native function. */
    public static SqlOperator bind(SqlOperator operator, List<? extends RexNode> args) {
        if (args.size() != 2 || !isOrdering(operator.getKind())
                || args.stream().noneMatch(arg -> containsUuid(arg.getType()))) return operator;
        var declaration = FunctionBindingDeclarations.sqlBinding(
                FunctionKind.FUNCTION_KIND_SCALAR, "", operator.getKind().name()).orElseThrow();
        return new UuidOrderingSqlFunction(operator.getKind(), declaration);
    }

    private static boolean isOrdering(SqlKind kind) {
        return switch (kind) {
            case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> true;
            default -> false;
        };
    }

    private static boolean containsUuid(RelDataType type) {
        if (type.getSqlTypeName() == SqlTypeName.UUID) return true;
        if (type.isStruct()) return type.getFieldList().stream().anyMatch(field -> containsUuid(field.getType()));
        return type.getComponentType() != null && containsUuid(type.getComponentType());
    }

    public SqlKind comparison() { return comparison; }
    public DeclaredFunctionReference declaration() { return declaration; }
}
