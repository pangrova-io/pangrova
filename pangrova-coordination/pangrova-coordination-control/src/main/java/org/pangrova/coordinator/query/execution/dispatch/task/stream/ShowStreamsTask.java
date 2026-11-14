package org.pangrova.coordinator.query.execution.dispatch.task.stream;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.parser.api.ast.statement.stream.ShowStreamsStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes SHOW STREAMS by delegating to {@link StreamDdlExecutor#show}.
 */
public final class ShowStreamsTask implements MetadataTask<ShowStreamsStatement> {

    private final StreamDdlExecutor executor;

    public ShowStreamsTask(StreamDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ShowStreamsStatement statement, StatementExecutionContext context) {
        String tenantId = resolveTenantId(context);
        List<TypedRow> rows = executor.show(tenantId).stream()
                .map(state -> TypedRow.builder()
                        .putString("stream_name", state.streamName())
                        .putString("tenant_id", state.tenantId())
                        .putString("state", state.lifecycleState().name())
                        .putString("target_table", state.targetTable())
                        .putString("source_format", state.sourceFormat())
                        .putString("created_at",
                                state.createdAt() != null ? state.createdAt().toString() : "")
                        .build())
                .toList();
        return StatementResult.fromRows(rows, null, StatementCategory.STREAM);
    }

    private static String resolveTenantId(StatementExecutionContext ctx) {
        return ctx.session() != null && ctx.session().tenantId() != null
                ? ctx.session().tenantId() : "";
    }
}
