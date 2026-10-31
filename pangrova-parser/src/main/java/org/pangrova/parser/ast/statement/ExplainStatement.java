package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents an EXPLAIN statement that returns the query execution plan.
 *
 * @param innerStatement the statement to explain
 */
public record ExplainStatement(Statement innerStatement) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.EXPLAIN;
    }
}
