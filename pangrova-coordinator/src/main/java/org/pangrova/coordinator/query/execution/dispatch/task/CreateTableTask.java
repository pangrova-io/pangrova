package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes CREATE TABLE by delegating to {@link DdlExecutor#executeCreateTable}.
 */
public final class CreateTableTask implements MetadataTask<CreateTableStatement> {

    private final DdlExecutor executor;

    public CreateTableTask(DdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(CreateTableStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeCreateTable(statement);
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.DDL);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
