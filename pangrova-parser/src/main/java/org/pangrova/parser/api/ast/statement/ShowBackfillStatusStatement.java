package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

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
