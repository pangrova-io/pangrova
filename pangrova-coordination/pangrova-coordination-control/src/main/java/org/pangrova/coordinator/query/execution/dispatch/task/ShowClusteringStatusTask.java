package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.coordinator.query.statement.ControlledTableMaintenance;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.parser.api.ast.statement.ShowClusteringStatusStatement;
import org.pangrova.types.value.TypedRow;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Executes SHOW CLUSTERING STATUS by delegating to {@link ControlledTableMaintenance#showClusteringStatus}.
 */
public final class ShowClusteringStatusTask implements MetadataTask<ShowClusteringStatusStatement> {

    private final BiFunction<ShowClusteringStatusStatement, QuerySession, List<TypedRow>> executor;

    public ShowClusteringStatusTask(BiFunction<ShowClusteringStatusStatement, QuerySession, List<TypedRow>> executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public StatementResult execute(ShowClusteringStatusStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.apply(statement, context.session());
        return StatementResult.fromRows(rows, null, StatementCategory.CLUSTERING);
    }
}
