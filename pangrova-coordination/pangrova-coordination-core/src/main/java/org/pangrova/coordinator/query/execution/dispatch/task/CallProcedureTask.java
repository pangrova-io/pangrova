package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.coordinator.query.statement.TableWarmupExecutor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
import org.pangrova.coordinator.query.statement.ControlledTableMaintenance;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes CALL procedure(args...) by resolving the catalog from the
 * procedure name prefix and enforcing the table-control maintenance boundary.
 *
 * <p>Procedure name format: {@code catalog.procedure_name}.
 * A catalog prefix is required.</p>
 */
public final class CallProcedureTask implements MetadataTask<CallStatement> {

    private static final Logger LOG = LoggerFactory.getLogger(CallProcedureTask.class);

    private final CatalogRegistry catalogRegistry;
    private final ControlledTableMaintenance maintenance;

    public CallProcedureTask(CatalogRegistry catalogRegistry, ControlledTableMaintenance maintenance) {
        this.catalogRegistry = catalogRegistry;
        this.maintenance = java.util.Objects.requireNonNull(maintenance, "maintenance");
    }

    @Override
    public StatementResult execute(CallStatement statement, StatementExecutionContext context) {
        List<String> nameParts = statement.procedureName().parts();
        String catalogName = resolveCatalog(nameParts);
        String procedureName = nameParts.get(nameParts.size() - 1);

        Map<String, String> args = evaluateArguments(statement.arguments());
        LOG.info("event=call_procedure catalog={} procedure={} argCount={}",
                catalogName, procedureName, args.size());

        // G-CACHE-06: intercept warmup_table and delegate to the engine-level
        // TableWarmupExecutor instead of the connector.
        if (TableWarmupExecutor.PROCEDURE_NAME.equalsIgnoreCase(procedureName)) {
            TableWarmupExecutor warmup = new TableWarmupExecutor(catalogRegistry);
            List<TypedRow> rows = warmup.execute(catalogName, args, context.session());
            return new StatementResult(
                    rows, extractColumnNames(rows), null, StatementCategory.MAINTENANCE);
        }

        List<TypedRow> rows = maintenance.executeProcedure(procedureName,
                TableResolver.resolveProcedureTableRef(statement), args, context.session());
        return new StatementResult(
                rows, extractColumnNames(rows), null, StatementCategory.MAINTENANCE);
    }

    private static String resolveCatalog(List<String> nameParts) {
        if (nameParts.size() >= 2) {
            return nameParts.get(0);
        }
        throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                "CALL requires a catalog prefix: CALL catalog.procedure_name(args...)");
    }

    private static Map<String, String> evaluateArguments(List<Expression> expressions) {
        Map<String, String> args = new LinkedHashMap<>();
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            String value = evaluateToString(expr, i);
            args.put("arg" + i, value);
        }
        return args;
    }

    private static String evaluateToString(Expression expr, int index) {
        if (expr instanceof Literal literal) {
            return literalValueToString(literal.value());
        }
        throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                "CALL argument at position " + index
                        + " must be a literal value, got: " + expr.getClass().getSimpleName());
    }

    private static String literalValueToString(LiteralValue val) {
        if (val instanceof LiteralValue.StringValue sv) {
            return sv.value();
        }
        if (val instanceof LiteralValue.IntegerValue iv) {
            return String.valueOf(iv.value());
        }
        if (val instanceof LiteralValue.DecimalValue dv) {
            return dv.value().toPlainString();
        }
        if (val instanceof LiteralValue.BooleanValue bv) {
            return String.valueOf(bv.value());
        }
        return val.toString();
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
