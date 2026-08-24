package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.clustering.ClusteringDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.parser.ast.statement.ReclusterTableStatement;
import org.pangrova.spi.connector.data.TypedRow;

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
        List<TypedRow> rows = executor.executeReclusterTable(statement);
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.CLUSTERING);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
