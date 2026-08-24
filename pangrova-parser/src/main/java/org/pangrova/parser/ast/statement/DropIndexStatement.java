package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents DROP INDEX [IF EXISTS] indexName ON tableName.
 */
public record DropIndexStatement(
        String indexName,
        Identifier tableName,
        boolean ifExists) implements Statement {
}
