package org.pangrova.parser.ast.statement.stream;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

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
