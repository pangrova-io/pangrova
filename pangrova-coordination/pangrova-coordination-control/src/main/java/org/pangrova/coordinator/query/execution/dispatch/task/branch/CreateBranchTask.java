package org.pangrova.coordinator.query.execution.dispatch.task.branch;

import java.util.List;

import org.pangrova.coordinator.ai.branch.BranchDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.statement.CreateBranchStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes CREATE BRANCH by delegating to {@link BranchDdlExecutor#executeCreateBranch}.
 */
public final class CreateBranchTask implements MetadataTask<CreateBranchStatement> {

    private final BranchDdlExecutor executor;

    public CreateBranchTask(BranchDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(CreateBranchStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeCreateBranch(statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.DDL);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
