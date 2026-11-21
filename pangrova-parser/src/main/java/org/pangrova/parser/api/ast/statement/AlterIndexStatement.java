package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

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
