package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.coordinator.query.statement.MaintenanceExecutor;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes COMPACT TABLE by delegating to {@link MaintenanceExecutor#executeCompact}.
 */
public final class CompactTask implements MetadataTask<CompactStatement> {

    private final MaintenanceExecutor executor;

    public CompactTask(MaintenanceExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(CompactStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeCompact(statement, context.session());
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.MAINTENANCE);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
