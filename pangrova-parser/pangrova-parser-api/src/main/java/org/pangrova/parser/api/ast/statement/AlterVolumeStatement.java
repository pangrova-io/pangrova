package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

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
