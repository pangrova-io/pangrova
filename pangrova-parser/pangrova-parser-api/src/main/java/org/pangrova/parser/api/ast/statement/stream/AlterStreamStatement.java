package org.pangrova.parser.api.ast.statement.stream;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents {@code ALTER STREAM <name> <action>}.
 */
public record AlterStreamStatement(
        Identifier name,
        AlterStreamAction action
) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.ALTER_STREAM;
    }
}
