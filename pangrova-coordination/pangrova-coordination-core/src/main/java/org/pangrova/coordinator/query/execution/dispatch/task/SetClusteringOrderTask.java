package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.clustering.ClusteringDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.query.core.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.statement.SetClusteringOrderStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes SET CLUSTERING ORDER by delegating to {@link ClusteringDdlExecutor#executeSetClusteringOrder}.
 */
public final class SetClusteringOrderTask implements MetadataTask<SetClusteringOrderStatement> {

    private final ClusteringDdlExecutor executor;

    public SetClusteringOrderTask(ClusteringDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(SetClusteringOrderStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeSetClusteringOrder(statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.CLUSTERING);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
