package org.pangrova.parser.api.ast.statement.maintenance;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents OPTIMIZE table statement.
 */
public record OptimizeStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.OPTIMIZE;
    }
}
