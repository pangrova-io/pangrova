package org.pangrova.coordinator.query.execution.dispatch;

import java.util.Set;

import org.pangrova.parser.api.ast.Statement;

/**
 * Factory for executing a specific category of SQL statements.
 * Each implementation handles one statement category (DDL, DML, EXPLAIN, etc.).
 *
 * @param <T> the statement type this factory handles
 */
public interface StatementExecutionFactory<T extends Statement> {

    /** Executes the statement and returns the result. */
    StatementResult execute(T statement, StatementExecutionContext context);

    /** Returns the concrete statement classes this factory handles. */
    Set<Class<? extends T>> supportedTypes();
}
