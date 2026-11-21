package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code ALTER DYNAMIC TABLE <name> <action>}.
 */
public record AlterDynamicTableStatement(
        Identifier name,
        AlterDynamicTableAction action
) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.ALTER_DYNAMIC_TABLE;
    }
}
