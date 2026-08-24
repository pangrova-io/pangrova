package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.index.ddl.IndexDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.parser.ast.statement.AlterIndexStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes ALTER INDEX by delegating to {@link IndexDdlExecutor#executeAlterIndex}.
 */
public final class AlterIndexTask implements MetadataTask<AlterIndexStatement> {

    private final IndexDdlExecutor executor;

    public AlterIndexTask(IndexDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(AlterIndexStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeAlterIndex(statement);
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.INDEX);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
