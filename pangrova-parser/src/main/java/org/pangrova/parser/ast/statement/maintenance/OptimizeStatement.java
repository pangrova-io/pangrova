package org.pangrova.parser.ast.statement.maintenance;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents OPTIMIZE table statement.
 */
public record OptimizeStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.OPTIMIZE;
    }
}
