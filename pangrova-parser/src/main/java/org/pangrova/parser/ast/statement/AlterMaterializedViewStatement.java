package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code ALTER MATERIALIZED VIEW <name> <action>}.
 */
public record AlterMaterializedViewStatement(
        Identifier name,
        AlterMaterializedViewAction action
) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.ALTER_MATERIALIZED_VIEW;
    }
}
