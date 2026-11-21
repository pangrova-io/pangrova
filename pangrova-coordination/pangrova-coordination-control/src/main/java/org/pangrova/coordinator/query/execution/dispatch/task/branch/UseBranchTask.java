package org.pangrova.coordinator.query.execution.dispatch.task.branch;

import org.pangrova.coordinator.ai.branch.BranchDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.parser.api.ast.statement.UseBranchStatement;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;

import java.util.List;

/** Acknowledges a validated connection-local branch change along with the query result. */
public record UseBranchTask(BranchDdlExecutor executor) implements MetadataTask<UseBranchStatement> {
    @Override
    public StatementResult execute(UseBranchStatement statement, StatementExecutionContext context) {
        context.stateMachine().selectBranch(executor.selectBranch(statement, context.session()));
        return new StatementResult(List.of(), List.of(), null, StatementCategory.SHOW);
    }
}
