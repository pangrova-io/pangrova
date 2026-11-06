package org.pangrova.parser.api.ast.statement.dml;

import java.util.Optional;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

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
