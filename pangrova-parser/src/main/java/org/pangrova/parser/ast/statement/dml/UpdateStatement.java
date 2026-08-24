package org.pangrova.parser.ast.statement.dml;

import java.util.List;
import java.util.Optional;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents UPDATE table SET col=val, ... [WHERE condition].
 */
public record UpdateStatement(
        Identifier tableName,
        List<Assignment> assignments,
        Optional<Expression> where) implements Statement {

    public UpdateStatement {
        assignments = List.copyOf(assignments);
    }

    /** Column assignment: column = expression. */
    public record Assignment(String column, Expression value) {}
}
