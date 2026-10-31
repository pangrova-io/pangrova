package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

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
