package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.ShowDescribeExecutor;
import org.pangrova.parser.api.ast.statement.ShowTableControlsStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes SHOW TABLE CONTROLS: ownership, identity, and version state for
 * LINKED/MANAGED tables, with live catalog tables merged in as OBSERVED.
 */
public final class ShowTableControlsTask
        implements MetadataTask<ShowTableControlsStatement> {

    private static final List<String> COLUMNS = List.of(
            "catalog_name", "schema_name", "table_name", "ownership_mode", "state",
            "table_id", "format", "format_table_uuid", "version_token",
            "metadata_revision", "provenance", "updated_at",
            "stats_version", "stats_row_count", "stats_data_size", "stats_computed_at");

    private final ShowDescribeExecutor executor;

    public ShowTableControlsTask(ShowDescribeExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ShowTableControlsStatement statement,
                                   StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeShowTableControls(statement, context.session());
        return new StatementResult(rows, COLUMNS, null, StatementCategory.SHOW);
    }
}
