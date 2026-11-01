package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.coordinator.query.statement.MaintenanceExecutor;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.spi.connector.data.value.TypedRow;

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
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.MAINTENANCE);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
