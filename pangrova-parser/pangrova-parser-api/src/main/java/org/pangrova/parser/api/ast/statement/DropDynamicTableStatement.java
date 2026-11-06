package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents {@code DROP DYNAMIC TABLE <name> [CASCADE]}.
 */
public record DropDynamicTableStatement(
        Identifier name,
        boolean cascade
) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_DYNAMIC_TABLE;
    }
}
