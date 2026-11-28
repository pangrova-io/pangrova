package org.pangrova.query.dispatch;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.pangrova.parser.api.ast.Statement;

/**
 * Factory for executing a specific category of SQL statements.
 * Each implementation handles one statement category (DDL, DML, EXPLAIN, etc.).
 * Implemented by coordinator-side orchestration; the contract itself belongs to
 * the query domain.
 *
 * @param <T> the statement type this factory handles
 */
public interface StatementExecutionFactory<T extends Statement> {

    /** Executes the statement and returns the result. */
    StatementResult execute(T statement, StatementExecutionContext context);

    /** Starts the statement and completes when its owned work has settled. */
    default CompletionStage<StatementResult> start(T statement, StatementExecutionContext context) {
        return CompletableFuture.completedFuture(execute(statement, context));
    }

    /** Whether work or result publication remains owned by this Coordinator after dispatch. */
    default boolean usesCoordinatorStatementAuthority(T statement) {
        return true;
    }

    /** Returns the concrete statement classes this factory handles. */
    Set<Class<? extends T>> supportedTypes();
}
