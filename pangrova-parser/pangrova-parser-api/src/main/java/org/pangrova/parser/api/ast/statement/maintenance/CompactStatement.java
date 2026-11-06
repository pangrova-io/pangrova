package org.pangrova.parser.api.ast.statement.maintenance;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents COMPACT table statement.
 */
public record CompactStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.COMPACT;
    }
}
