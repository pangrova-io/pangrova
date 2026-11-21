package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.clustering.ClusteringDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.statement.ReclusterTableStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes RECLUSTER TABLE by delegating to {@link ClusteringDdlExecutor#executeReclusterTable}.
 */
public final class ReclusterTableTask implements MetadataTask<ReclusterTableStatement> {

    private final ClusteringDdlExecutor executor;

    public ReclusterTableTask(ClusteringDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ReclusterTableStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeReclusterTable(statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return StatementResult.fromRows(rows, mutatedTable, StatementCategory.CLUSTERING);
    }
}
