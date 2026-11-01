package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.parser.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.ast.statement.stream.DescribeStreamStatement;
import org.pangrova.parser.ast.statement.stream.DropStreamStatement;
import org.pangrova.spi.connector.data.value.TypedRow;

import java.util.List;

/**
 * Executes stream DDL statements (CREATE/ALTER/DROP/SHOW/DESCRIBE STREAM)
 * and converts results to TypedRow format.
 */
final class StreamStatementExecutor {

    private StreamStatementExecutor() {
    }

    static List<TypedRow> create(StreamDdlExecutor executor,
                                 CreateStreamStatement stmt,
                                 CoordinatorSessionContext ctx) {
        String tenantId = ctx != null ? ctx.tenantId() : "";
        String creator = ctx != null && ctx.userIdentity() != null
                ? ctx.userIdentity().principal() : "anonymous";
        executor.create(stmt, tenantId, creator);
        return List.of(TypedRow.builder().putString("result", "CREATE STREAM succeeded").build());
    }

    static List<TypedRow> alter(StreamDdlExecutor executor,
                                AlterStreamStatement stmt,
                                CoordinatorSessionContext ctx) {
        String tenantId = ctx != null ? ctx.tenantId() : "";
        executor.alter(stmt, tenantId);
        return List.of(TypedRow.builder().putString("result", "ALTER STREAM succeeded").build());
    }

    static List<TypedRow> drop(StreamDdlExecutor executor,
                               DropStreamStatement stmt,
                               CoordinatorSessionContext ctx) {
        String tenantId = ctx != null ? ctx.tenantId() : "";
        executor.drop(stmt, tenantId);
        return List.of(TypedRow.builder().putString("result", "DROP STREAM succeeded").build());
    }

    static List<TypedRow> show(StreamDdlExecutor executor, CoordinatorSessionContext ctx) {
        String tenantId = ctx != null ? ctx.tenantId() : "";
        return executor.show(tenantId).stream()
                .map(state -> TypedRow.builder()
                        .putString("stream_name", state.streamName())
                        .putString("tenant_id", state.tenantId())
                        .putString("state", state.lifecycleState().name())
                        .putString("target_table", state.targetTable())
                        .putString("source_format", state.sourceFormat())
                        .putString("created_at", state.createdAt() != null ? state.createdAt().toString() : "")
                        .build())
                .toList();
    }

    static List<TypedRow> describe(StreamDdlExecutor executor,
                                   DescribeStreamStatement stmt,
                                   CoordinatorSessionContext ctx) {
        String tenantId = ctx != null ? ctx.tenantId() : "";
        String streamName = stmt.name().parts().get(stmt.name().parts().size() - 1);
        return executor.describe(streamName, tenantId)
                .map(state -> TypedRow.builder()
                        .putString("stream_name", state.streamName())
                        .putString("tenant_id", state.tenantId())
                        .putString("state", state.lifecycleState().name())
                        .putString("target_table", state.targetTable())
                        .putString("source_format", state.sourceFormat())
                        .putString("source_properties", state.sourceProperties().toString())
                        .putString("stream_properties", state.streamProperties().toString())
                        .putString("created_at", state.createdAt() != null ? state.createdAt().toString() : "")
                        .putString("updated_at", state.updatedAt() != null ? state.updatedAt().toString() : "")
                        .putString("error_message", state.errorMessage() != null ? state.errorMessage() : "")
                        .build())
                .map(List::of)
                .orElse(List.of());
    }
}
