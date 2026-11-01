package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.clustering.ClusteringDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.ShowClusteringStatusStatement;
import org.pangrova.spi.connector.data.value.TypedRow;

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
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.CLUSTERING);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
