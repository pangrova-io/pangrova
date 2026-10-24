package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code DESCRIBE MATERIALIZED VIEW <name>}.
 */
public record DescribeMaterializedViewStatement(
        Identifier name
) implements Statement {
}
