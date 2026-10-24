package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

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
}
