package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code REFRESH MATERIALIZED VIEW <name>}.
 */
public record RefreshMaterializedViewStatement(
        Identifier name
) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.REFRESH_MATERIALIZED_VIEW;
    }
}
