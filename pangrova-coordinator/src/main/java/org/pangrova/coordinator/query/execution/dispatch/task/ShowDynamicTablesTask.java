package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.dynamictable.DynamicTableDdlExecutor;
import org.pangrova.coordinator.dynamictable.DynamicTableInfo;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.ShowDynamicTablesStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes SHOW DYNAMIC TABLES by delegating to {@link DynamicTableDdlExecutor#show}.
 */
public final class ShowDynamicTablesTask implements MetadataTask<ShowDynamicTablesStatement> {

    private final DynamicTableDdlExecutor executor;

    public ShowDynamicTablesTask(DynamicTableDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ShowDynamicTablesStatement statement, StatementExecutionContext context) {
        String tenantId = context.session() != null ? context.session().tenantId() : null;
        List<DynamicTableInfo> infos = executor.show(tenantId);
        List<TypedRow> rows = infos.stream().map(ShowDynamicTablesTask::infoToRow).toList();
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.DYNAMIC_TABLE);
    }

    private static TypedRow infoToRow(DynamicTableInfo info) {
        return TypedRow.builder()
                .putString("dt_name", info.dtName())
                .putString("tenant_id", info.tenantId())
                .putString("state", info.state().name())
                .putString("refresh_mode", info.refreshMode().name())
                .putString("target_lag", info.targetLag().toString())
                .putString("last_refresh_time",
                        info.lastRefreshTime() != null ? info.lastRefreshTime().toString() : "")
                .putLong("total_refresh_count", info.totalRefreshCount())
                .build();
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
