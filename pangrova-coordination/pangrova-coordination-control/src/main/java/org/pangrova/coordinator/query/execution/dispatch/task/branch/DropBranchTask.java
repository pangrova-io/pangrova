package org.pangrova.coordinator.query.execution.dispatch.task.branch;

import java.util.List;

import org.pangrova.coordinator.ai.branch.BranchDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.statement.DropBranchStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes DROP BRANCH by delegating to {@link BranchDdlExecutor#executeDropBranch}.
 */
public final class DropBranchTask implements MetadataTask<DropBranchStatement> {

    private final BranchDdlExecutor executor;

    public DropBranchTask(BranchDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(DropBranchStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeDropBranch(statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return StatementResult.fromRows(rows, mutatedTable, StatementCategory.DDL);
    }
}
