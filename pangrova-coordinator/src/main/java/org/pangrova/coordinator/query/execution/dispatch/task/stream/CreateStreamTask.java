package org.pangrova.coordinator.query.execution.dispatch.task.stream;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.streaming.StreamDdlExecutor;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes CREATE STREAM by delegating to {@link StreamDdlExecutor#create}.
 */
public final class CreateStreamTask implements MetadataTask<CreateStreamStatement> {

    private final StreamDdlExecutor executor;

    public CreateStreamTask(StreamDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(CreateStreamStatement statement, StatementExecutionContext context) {
        String tenantId = resolveTenantId(context);
        String creator = context.session() != null && context.session().userIdentity() != null
                ? context.session().userIdentity().principal() : "anonymous";
        executor.create(statement, tenantId, creator);
        List<TypedRow> rows = List.of(TypedRow.builder().putString("result", "CREATE STREAM succeeded").build());
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
