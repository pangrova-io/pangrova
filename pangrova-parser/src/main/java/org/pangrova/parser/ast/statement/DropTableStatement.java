package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents DROP TABLE [IF EXISTS] qualifiedName.
 */
public record DropTableStatement(Identifier tableName, boolean ifExists) implements Statement {
}
