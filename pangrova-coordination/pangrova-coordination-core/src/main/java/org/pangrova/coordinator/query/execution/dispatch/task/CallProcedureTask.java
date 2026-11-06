package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.query.core.statement.TableProcedureCall;
import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.TableWarmupExecutor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.parser.api.ast.statement.maintenance.CallStatement;
import org.pangrova.coordinator.query.statement.ControlledTableMaintenance;
import org.pangrova.types.value.TypedRow;
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
        var call = TableProcedureCall.bind(statement);
        LOG.info("event=call_procedure catalog={} procedure={} argCount={}",
                call.target().catalog(), call.procedureName(), call.arguments().size());
        if (call.isWarmup()) {
            TableWarmupExecutor warmup = new TableWarmupExecutor(catalogRegistry);
            List<TypedRow> rows = warmup.execute(call.target(), context.session());
            return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.MAINTENANCE);
        }
        List<TypedRow> rows = maintenance.executeProcedure(
                call.procedureName(), call.target(), call.arguments(), context.session());
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.MAINTENANCE);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
