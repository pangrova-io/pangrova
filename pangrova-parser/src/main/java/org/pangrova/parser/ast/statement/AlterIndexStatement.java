package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents ALTER INDEX indexName ON tableName REBUILD.
 */
public record AlterIndexStatement(
        String indexName,
        Identifier tableName) implements Statement {
}
