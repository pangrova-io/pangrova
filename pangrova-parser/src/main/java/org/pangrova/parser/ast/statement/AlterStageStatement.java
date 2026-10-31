package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

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
