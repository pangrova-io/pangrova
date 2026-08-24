package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;

/**
 * Represents an EXPLAIN statement that returns the query execution plan.
 *
 * @param innerStatement the statement to explain
 */
public record ExplainStatement(Statement innerStatement) implements Statement {
}
