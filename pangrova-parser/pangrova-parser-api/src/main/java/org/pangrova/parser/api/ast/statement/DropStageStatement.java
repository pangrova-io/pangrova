package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents {@code DROP STAGE [IF EXISTS] name}.
 */
public record DropStageStatement(Identifier name, boolean ifExists) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_STAGE;
    }
}
