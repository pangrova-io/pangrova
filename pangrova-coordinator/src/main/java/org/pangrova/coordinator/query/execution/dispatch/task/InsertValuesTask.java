package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.coordinator.query.statement.dml.DmlExecutor;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes INSERT VALUES by delegating to {@link DmlExecutor#executeInsert}.
 */
public final class InsertValuesTask implements MetadataTask<InsertStatement> {

    private final DmlExecutor executor;

    public InsertValuesTask(DmlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(InsertStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeInsert(statement);
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.DML);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
