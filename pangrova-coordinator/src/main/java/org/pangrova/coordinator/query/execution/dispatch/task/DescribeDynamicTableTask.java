package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.dynamictable.DynamicTableDdlExecutor;
import org.pangrova.coordinator.dynamictable.DynamicTableDetail;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes DESCRIBE DYNAMIC TABLE by delegating to {@link DynamicTableDdlExecutor#describe}.
 */
public final class DescribeDynamicTableTask implements MetadataTask<DescribeDynamicTableStatement> {

    private final DynamicTableDdlExecutor executor;

    public DescribeDynamicTableTask(DynamicTableDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(DescribeDynamicTableStatement statement, StatementExecutionContext context) {
        String name = statement.name().parts().get(statement.name().parts().size() - 1);
        DynamicTableDetail detail = executor.describe(name);
        List<TypedRow> rows = List.of(detailToRow(detail));
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.DYNAMIC_TABLE);
    }

    private static TypedRow detailToRow(DynamicTableDetail detail) {
        return TypedRow.builder()
                .putString("dt_name", detail.dtName())
                .putString("tenant_id", detail.tenantId())
                .putString("state", detail.state().name())
                .putString("refresh_mode", detail.refreshMode().name())
                .putString("target_lag", detail.targetLag().toString())
                .putString("checkpoint", detail.checkpoint() != null ? detail.checkpoint() : "")
                .putString("checkpoint_type", detail.checkpointType() != null ? detail.checkpointType() : "")
                .putInt("source_schema_version", detail.sourceSchemaVersion())
                .putString("creator", detail.creatorIdentity() != null ? detail.creatorIdentity() : "")
                .putString("last_refresh_time",
                        detail.lastRefreshTime() != null ? detail.lastRefreshTime().toString() : "")
                .putLong("total_refresh_count", detail.totalRefreshCount())
                .putInt("consecutive_failures", detail.consecutiveFailures())
                .putString("created_at", detail.createdAt().toString())
                .putInt("recent_refresh_count", detail.recentRefreshes().size())
                .build();
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
