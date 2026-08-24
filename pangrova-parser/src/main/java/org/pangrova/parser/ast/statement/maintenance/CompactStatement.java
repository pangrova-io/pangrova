package org.pangrova.parser.ast.statement.maintenance;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents COMPACT table statement.
 */
public record CompactStatement(Identifier tableName) implements Statement {
}
