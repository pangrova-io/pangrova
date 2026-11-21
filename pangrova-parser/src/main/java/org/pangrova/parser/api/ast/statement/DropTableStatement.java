package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents DROP TABLE [IF EXISTS] qualifiedName.
 */
public record DropTableStatement(Identifier tableName, boolean ifExists) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_TABLE;
    }
}
