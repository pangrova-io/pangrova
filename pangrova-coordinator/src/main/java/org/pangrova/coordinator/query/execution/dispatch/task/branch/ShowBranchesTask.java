package org.pangrova.coordinator.query.execution.dispatch.task.branch;

import java.util.List;

import org.pangrova.coordinator.ai.branch.BranchDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.ShowBranchesStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Executes SHOW BRANCHES by delegating to {@link BranchDdlExecutor#executeShowBranches}.
 */
public final class ShowBranchesTask implements MetadataTask<ShowBranchesStatement> {

    private final BranchDdlExecutor executor;

    public ShowBranchesTask(BranchDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(ShowBranchesStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeShowBranches(statement);
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.SHOW);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
