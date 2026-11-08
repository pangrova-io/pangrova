package org.pangrova.coordinator.query.execution.dispatch.task.stream;

import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.parser.api.ast.statement.stream.DescribeStreamStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes DESCRIBE STREAM by delegating to {@link StreamDdlExecutor#describe}.
 */
public final class DescribeStreamTask implements MetadataTask<DescribeStreamStatement> {

    private final StreamDdlExecutor executor;

    public DescribeStreamTask(StreamDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(DescribeStreamStatement statement, StatementExecutionContext context) {
        String tenantId = resolveTenantId(context);
        String streamName = statement.name().parts().get(statement.name().parts().size() - 1);
        Optional<StreamState> state = executor.describe(streamName, tenantId);
        List<TypedRow> rows = state.map(DescribeStreamTask::toRow)
                .map(List::of)
                .orElse(List.of());
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.STREAM);
    }

    private static TypedRow toRow(StreamState s) {
        return TypedRow.builder()
                .putString("stream_name", s.streamName())
                .putString("tenant_id", s.tenantId())
                .putString("state", s.lifecycleState().name())
                .putString("target_table", s.targetTable())
                .putString("source_format", s.sourceFormat())
                .putString("source_properties", s.sourceProperties().toString())
                .putString("stream_properties", s.streamProperties().toString())
                .putString("created_at",
                        s.createdAt() != null ? s.createdAt().toString() : "")
                .putString("updated_at",
                        s.updatedAt() != null ? s.updatedAt().toString() : "")
                .putString("error_message",
                        s.errorMessage() != null ? s.errorMessage() : "")
                .build();
    }

    private static String resolveTenantId(StatementExecutionContext ctx) {
        return ctx.session() != null && ctx.session().tenantId() != null
                ? ctx.session().tenantId() : "";
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
