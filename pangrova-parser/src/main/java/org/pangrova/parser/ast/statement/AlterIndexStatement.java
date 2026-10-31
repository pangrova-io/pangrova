package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents ALTER INDEX indexName ON tableName REBUILD.
 */
public record AlterIndexStatement(
        String indexName,
        Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.ALTER_INDEX;
    }
}
