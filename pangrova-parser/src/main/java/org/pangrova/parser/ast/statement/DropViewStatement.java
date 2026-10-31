package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents DROP VIEW [IF EXISTS] name.
 */
public record DropViewStatement(
        Identifier viewName,
        boolean ifExists) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_VIEW;
    }
}
