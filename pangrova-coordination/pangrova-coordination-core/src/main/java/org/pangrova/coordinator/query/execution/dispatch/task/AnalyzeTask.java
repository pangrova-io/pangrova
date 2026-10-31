package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.coordinator.query.statement.AnalyzeExecutor;
import org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement;
import org.pangrova.connector.spi.data.value.TypedRow;

/**
 * Executes ANALYZE TABLE by delegating to {@link AnalyzeExecutor}.
 */
public final class AnalyzeTask implements MetadataTask<AnalyzeStatement> {

    private final AnalyzeExecutor executor;

    public AnalyzeTask(AnalyzeExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(AnalyzeStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.execute(statement, context.session());
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable,
                StatementCategory.MAINTENANCE);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
