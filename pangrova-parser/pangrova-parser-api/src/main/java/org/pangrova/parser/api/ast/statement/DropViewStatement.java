package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

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
