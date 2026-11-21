package org.pangrova.query.statement;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.LiteralValue;
import org.pangrova.parser.api.ast.statement.maintenance.CallStatement;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Exact target and literal arguments shared by CALL admission and execution.
 * Binding performs no lookup, authorization or provider discovery. A valid binding
 * does not imply that a connector procedure is supported or that its caller may run it.
 */
@Internal
public record TableProcedureCall(String procedureName, QualifiedTableName target, Map<String, String> arguments) {
    public static final String WARMUP_PROCEDURE_NAME = "warmup_table";

    public TableProcedureCall {
        Objects.requireNonNull(procedureName, "procedureName");
        Objects.requireNonNull(target, "target");
        arguments = Map.copyOf(arguments);
    }

    /** The built-in warmup reads rows; all connector procedures retain the maintenance mutation policy. */
    public boolean isWarmup() {
        return WARMUP_PROCEDURE_NAME.equalsIgnoreCase(procedureName);
    }

    /** Binds catalog[.namespace].procedure; the target table always belongs to that exact catalog. */
    public static TableProcedureCall bind(CallStatement statement) {
        Objects.requireNonNull(statement, "statement");
        List<String> name = statement.procedureName().parts();
        if (name.size() < 2 || name.size() > 3) {
            throw invalid("CALL requires catalog[.schema].procedure_name");
        }
        String procedure = name.get(name.size() - 1);
        boolean warmup = WARMUP_PROCEDURE_NAME.equalsIgnoreCase(procedure);
        var values = statement.arguments();
        if (warmup && (values.size() < 1 || values.size() > 2)) {
            throw invalid("warmup_table requires arguments: 'schema.table' or 'schema', 'table'");
        }
        String first = stringArgument(values, 0);
        QualifiedTableName target;
        if (warmup && values.size() == 2) {
            target = new QualifiedTableName(name.get(0), first, stringArgument(values, 1));
        } else {
            String[] parts = first.split("\\.", -1);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                throw invalid("CALL table argument must be 'schema.table'; use two warmup arguments for literal dots");
            }
            target = new QualifiedTableName(name.get(0), parts[0], parts[1]);
        }
        var arguments = new LinkedHashMap<String, String>();
        for (int i = 0; i < values.size(); i++) {
            arguments.put("arg" + i, literalArgument(values.get(i), i));
        }
        return new TableProcedureCall(procedure, target, arguments);
    }

    private static String stringArgument(List<Expression> values, int index) {
        if (values.size() > index && values.get(index) instanceof Literal literal
                && literal.value() instanceof LiteralValue.StringValue string && !string.value().isBlank()) {
            return string.value();
        }
        throw invalid("CALL table argument must be a nonempty string literal");
    }

    private static String literalArgument(Expression expression, int index) {
        if (!(expression instanceof Literal literal)) {
            throw invalid("CALL argument at position " + index + " must be a literal value, got: "
                    + expression.getClass().getSimpleName());
        }
        LiteralValue value = literal.value();
        if (value instanceof LiteralValue.StringValue string) return string.value();
        if (value instanceof LiteralValue.IntegerValue integer) return String.valueOf(integer.value());
        if (value instanceof LiteralValue.DecimalValue decimal) return decimal.value().toPlainString();
        if (value instanceof LiteralValue.BooleanValue bool) return String.valueOf(bool.value());
        return value.toString();
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
