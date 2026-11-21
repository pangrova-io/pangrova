package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.clustering.ClusteringDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.parser.api.ast.statement.ShowClusteringStatusStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes SHOW CLUSTERING STATUS by delegating to {@link ClusteringDdlExecutor#executeShowClusteringStatus}.
 */
public final class ShowClusteringStatusTask implements MetadataTask<ShowClusteringStatusStatement> {

    private final ClusteringDdlExecutor executor;

    public ShowClusteringStatusTask(ClusteringDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ShowClusteringStatusStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeShowClusteringStatus(statement, context.session());
        return StatementResult.fromRows(rows, null, StatementCategory.CLUSTERING);
    }
}
