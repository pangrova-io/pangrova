package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents SHOW TAGS ON table.
 */
public record ShowTagsStatement(Identifier tableName) implements Statement {
}
