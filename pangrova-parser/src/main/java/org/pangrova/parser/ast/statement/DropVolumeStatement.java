package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code DROP VOLUME [IF EXISTS] name}.
 */
public record DropVolumeStatement(Identifier name, boolean ifExists) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_VOLUME;
    }
}
