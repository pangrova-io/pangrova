package org.pangrova.coordinator.query.execution.dispatch;
import org.pangrova.query.core.dispatch.StatementResult;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.query.core.dispatch.StatementExecutionContext;

/**
 * Executes a single metadata operation (CREATE TABLE, DROP TABLE, SHOW TABLES, etc.).
 *
 * <p>Each concrete statement type gets its own {@code MetadataTask} implementation,
 * registered with {@link MetadataExecutionFactory}. Adding a new statement type
 * requires only a new task implementation and a single registration call —
 * no new factory class.
 *
 * @param <T> the concrete statement type this task handles
 */
public interface MetadataTask<T extends Statement> {

    /** Executes the metadata operation and returns the result. */
    StatementResult execute(T statement, StatementExecutionContext context);
}
