package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code REFRESH MATERIALIZED VIEW <name>}.
 */
public record RefreshMaterializedViewStatement(
        Identifier name
) implements Statement {
}
