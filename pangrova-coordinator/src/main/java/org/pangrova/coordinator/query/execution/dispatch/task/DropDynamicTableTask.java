package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.dynamictable.DynamicTableDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes DROP DYNAMIC TABLE by delegating to {@link DynamicTableDdlExecutor#drop}.
 */
public final class DropDynamicTableTask implements MetadataTask<DropDynamicTableStatement> {

    private final DynamicTableDdlExecutor executor;

    public DropDynamicTableTask(DynamicTableDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(DropDynamicTableStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = DynamicTableResultConverter.toRows(executor.drop(statement, context.session()));
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.DYNAMIC_TABLE);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
