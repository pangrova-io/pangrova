package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

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
