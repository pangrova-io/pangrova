package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

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
