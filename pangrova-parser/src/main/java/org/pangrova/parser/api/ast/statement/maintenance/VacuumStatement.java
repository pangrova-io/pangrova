package org.pangrova.parser.api.ast.statement.maintenance;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents VACUUM table statement.
 */
public record VacuumStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.VACUUM;
    }
}
