package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.dynamictable.DynamicTableDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes ALTER DYNAMIC TABLE by delegating to {@link DynamicTableDdlExecutor#alter}.
 */
public final class AlterDynamicTableTask implements MetadataTask<AlterDynamicTableStatement> {

    private final DynamicTableDdlExecutor executor;

    public AlterDynamicTableTask(DynamicTableDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(AlterDynamicTableStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = DynamicTableResultConverter.toRows(executor.alter(statement, context.session()));
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.DYNAMIC_TABLE);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
