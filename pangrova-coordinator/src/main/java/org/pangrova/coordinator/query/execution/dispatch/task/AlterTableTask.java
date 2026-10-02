package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes ALTER TABLE by delegating to {@link DdlExecutor#executeAlterTable}.
 */
public final class AlterTableTask implements MetadataTask<AlterTableStatement> {

    private final DdlExecutor executor;

    public AlterTableTask(DdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(AlterTableStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeAlterTable(
                statement, context.session());
        String mutatedTable = TableNameExtractor.extractQualified(statement, context.session()).toString();
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.DDL);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
