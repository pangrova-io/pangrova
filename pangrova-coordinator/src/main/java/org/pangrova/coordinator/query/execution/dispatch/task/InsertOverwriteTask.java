package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.coordinator.query.statement.dml.DmlExecutor;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes INSERT OVERWRITE by delegating to {@link DmlExecutor#executeInsertOverwrite}.
 */
public final class InsertOverwriteTask implements MetadataTask<InsertOverwriteStatement> {

    private final DmlExecutor executor;

    public InsertOverwriteTask(DmlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(InsertOverwriteStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeInsertOverwrite(statement);
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.DML);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
