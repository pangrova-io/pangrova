package org.pangrova.parser.ast.statement.dml;

import java.util.Optional;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents DELETE FROM table [WHERE condition].
 */
public record DeleteStatement(
        Identifier tableName,
        Optional<Expression> where) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DELETE;
    }
}
