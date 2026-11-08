package org.pangrova.coordinator.query.execution.dispatch.task.stream;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.parser.api.ast.statement.stream.DropStreamStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes DROP STREAM by delegating to {@link StreamDdlExecutor#drop}.
 */
public final class DropStreamTask implements MetadataTask<DropStreamStatement> {

    private final StreamDdlExecutor executor;

    public DropStreamTask(StreamDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(DropStreamStatement statement, StatementExecutionContext context) {
        String tenantId = resolveTenantId(context);
        executor.drop(statement, tenantId);
        List<TypedRow> rows = List.of(TypedRow.builder().putString("result", "DROP STREAM succeeded").build());
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.STREAM);
    }

    private static String resolveTenantId(StatementExecutionContext ctx) {
        return ctx.session() != null && ctx.session().tenantId() != null
                ? ctx.session().tenantId() : "";
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
