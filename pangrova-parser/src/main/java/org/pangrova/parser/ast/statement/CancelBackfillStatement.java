package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents CANCEL BACKFILL ON table COLUMN col.
 *
 * <p>Cancels an in-progress backfill operation for the specified column.
 */
public record CancelBackfillStatement(
        Identifier tableName,
        String columnName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.CANCEL_BACKFILL;
    }
}
