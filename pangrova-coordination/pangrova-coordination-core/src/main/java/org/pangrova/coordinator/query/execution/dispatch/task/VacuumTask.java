package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.query.core.statement.StatementTableTargets;
import org.pangrova.coordinator.query.statement.MaintenanceExecutor;
import org.pangrova.parser.api.ast.statement.maintenance.VacuumStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes VACUUM TABLE by delegating to {@link MaintenanceExecutor#executeVacuum}.
 */
public final class VacuumTask implements MetadataTask<VacuumStatement> {

    private final MaintenanceExecutor executor;

    public VacuumTask(MaintenanceExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(VacuumStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeVacuum(statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.MAINTENANCE);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
