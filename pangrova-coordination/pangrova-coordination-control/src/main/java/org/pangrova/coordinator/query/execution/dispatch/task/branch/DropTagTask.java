package org.pangrova.coordinator.query.execution.dispatch.task.branch;

import java.util.List;

import org.pangrova.coordinator.ai.branch.BranchDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.statement.DropTagStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Executes DROP TAG by delegating to {@link BranchDdlExecutor#executeDropTag}.
 */
public final class DropTagTask implements MetadataTask<DropTagStatement> {

    private final BranchDdlExecutor executor;

    public DropTagTask(BranchDdlExecutor executor) {
        this.executor = executor;
    }

    @Override
    public StatementResult execute(DropTagStatement statement, StatementExecutionContext context) {
        List<TypedRow> rows = executor.executeDropTag(statement, context.session());
        String mutatedTable = StatementTableTargets.resolve(statement, context.session()).toString();
        return StatementResult.fromRows(rows, mutatedTable, StatementCategory.DDL);
    }
}
