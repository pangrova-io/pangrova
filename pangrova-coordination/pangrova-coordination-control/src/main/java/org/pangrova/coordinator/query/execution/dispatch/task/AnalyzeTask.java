package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.coordinator.query.statement.AnalyzeExecutor;
import org.pangrova.parser.api.ast.statement.maintenance.AnalyzeStatement;
import org.pangrova.types.value.TypedRow;

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
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return StatementResult.fromRows(rows, mutatedTable,
                StatementCategory.MAINTENANCE);
    }
}
