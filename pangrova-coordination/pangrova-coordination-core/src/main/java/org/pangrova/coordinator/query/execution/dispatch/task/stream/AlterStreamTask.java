package org.pangrova.coordinator.query.execution.dispatch.task.stream;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.parser.ast.statement.stream.AlterStreamStatement;
import org.pangrova.spi.connector.data.value.TypedRow;

/**
 * Executes ALTER STREAM by delegating to {@link StreamDdlExecutor#alter}.
 */
public final class AlterStreamTask implements MetadataTask<AlterStreamStatement> {

    private final StreamDdlExecutor executor;

    public AlterStreamTask(StreamDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(AlterStreamStatement statement, StatementExecutionContext context) {
        String tenantId = resolveTenantId(context);
        executor.alter(statement, tenantId);
        List<TypedRow> rows = List.of(TypedRow.builder().putString("result", "ALTER STREAM succeeded").build());
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
