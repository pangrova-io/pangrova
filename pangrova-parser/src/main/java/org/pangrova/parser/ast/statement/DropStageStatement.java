package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code DROP STAGE [IF EXISTS] name}.
 */
public record DropStageStatement(Identifier name, boolean ifExists) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_STAGE;
    }
}
