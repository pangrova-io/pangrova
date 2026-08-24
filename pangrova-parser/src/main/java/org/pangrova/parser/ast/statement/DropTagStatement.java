package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents DROP TAG name ON table.
 */
public record DropTagStatement(
        String tagName,
        Identifier tableName) implements Statement {
}
