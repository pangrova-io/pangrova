package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.coordinator.query.statement.ControlledTableMaintenance;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.statement.ReclusterTableStatement;
import org.pangrova.types.value.TypedRow;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Executes RECLUSTER TABLE by delegating to {@link ControlledTableMaintenance#recluster}.
 */
public final class ReclusterTableTask implements MetadataTask<ReclusterTableStatement> {

    private final BiFunction<ReclusterTableStatement, QuerySession, List<TypedRow>> executor;

    public ReclusterTableTask(BiFunction<ReclusterTableStatement, QuerySession, List<TypedRow>> executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public StatementResult execute(ReclusterTableStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.apply(statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return StatementResult.fromRows(rows, mutatedTable, StatementCategory.CLUSTERING);
    }
}
