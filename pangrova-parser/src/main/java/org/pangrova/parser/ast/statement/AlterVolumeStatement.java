package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code ALTER VOLUME <name> <action>}.
 */
public record AlterVolumeStatement(
        Identifier name,
        AlterVolumeAction action) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.ALTER_VOLUME;
    }
}
