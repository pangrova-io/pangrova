package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

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
