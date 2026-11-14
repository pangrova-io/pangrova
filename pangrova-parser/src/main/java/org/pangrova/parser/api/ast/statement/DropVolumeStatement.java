package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code DROP VOLUME [IF EXISTS] name}.
 */
public record DropVolumeStatement(Identifier name, boolean ifExists) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_VOLUME;
    }
}
