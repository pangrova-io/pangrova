package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents {@code DROP MATERIALIZED VIEW [IF EXISTS] <name> [CASCADE]}.
 *
 * @param name object name
 * @param cascade whether transitive dependents are dropped first
 * @param ifExists whether a missing object is a no-op
 */
public record DropMaterializedViewStatement(
        Identifier name,
        boolean cascade,
        boolean ifExists
) implements Statement {

    public DropMaterializedViewStatement(Identifier name, boolean cascade) {
        this(name, cascade, false);
    }

    @Override
    public StatementType type() {
        return StatementType.DROP_MATERIALIZED_VIEW;
    }
}
