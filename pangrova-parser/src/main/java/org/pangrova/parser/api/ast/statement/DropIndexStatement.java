package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents DROP INDEX [IF EXISTS] indexName ON tableName.
 */
public record DropIndexStatement(
        String indexName,
        Identifier tableName,
        boolean ifExists) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_INDEX;
    }
}
