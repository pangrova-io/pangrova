package org.pangrova.coordinator.query.execution.dispatch.task.branch;

import java.util.List;

import org.pangrova.coordinator.ai.branch.BranchDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.parser.ast.statement.DropTagStatement;
import org.pangrova.spi.connector.data.value.TypedRow;

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
        String mutatedTable = TableNameExtractor.extractQualified(statement, context.session()).toString();
        return new StatementResult(rows, extractColumnNames(rows), mutatedTable, StatementCategory.DDL);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
