package org.pangrova.coordinator.query.execution.dispatch.task.stream;

import java.util.List;

import org.pangrova.coordinator.dynamictable.DynamicTableDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.coordinator.query.execution.dispatch.task.DynamicTableResultConverter;
import org.pangrova.parser.ast.CreateContinuousQueryStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes CREATE CONTINUOUS QUERY by delegating to {@link DynamicTableDdlExecutor#createContinuousQuery}.
 */
public final class CreateContinuousQueryTask implements MetadataTask<CreateContinuousQueryStatement> {

    private final DynamicTableDdlExecutor executor;

    public CreateContinuousQueryTask(DynamicTableDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(CreateContinuousQueryStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = DynamicTableResultConverter.toRows(
                executor.createContinuousQuery(statement, context.session()));
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.DYNAMIC_TABLE);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
