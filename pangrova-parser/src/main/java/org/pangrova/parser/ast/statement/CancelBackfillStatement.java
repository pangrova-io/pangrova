package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents CANCEL BACKFILL ON table COLUMN col.
 *
 * <p>Cancels an in-progress backfill operation for the specified column.
 */
public record CancelBackfillStatement(
        Identifier tableName,
        String columnName) implements Statement {
}
