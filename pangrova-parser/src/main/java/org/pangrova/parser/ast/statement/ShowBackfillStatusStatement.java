package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents SHOW BACKFILL STATUS ON table.
 *
 * <p>Returns the current status of backfill operations on the specified table.
 */
public record ShowBackfillStatusStatement(Identifier tableName) implements Statement {
}
