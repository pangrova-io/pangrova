package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents SHOW BACKFILL STATUS ON table.
 *
 * <p>Returns the current status of backfill operations on the specified table.
 */
public record ShowBackfillStatusStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_BACKFILL_STATUS;
    }
}
