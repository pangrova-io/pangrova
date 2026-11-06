package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents {@code ALTER STAGE <name> <action>}.
 */
public record AlterStageStatement(
        Identifier name,
        AlterStageAction action) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.ALTER_STAGE;
    }
}
