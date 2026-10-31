package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

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
