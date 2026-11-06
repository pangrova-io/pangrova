package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents {@code DESCRIBE MATERIALIZED VIEW <name>}.
 */
public record DescribeMaterializedViewStatement(
        Identifier name
) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_MATERIALIZED_VIEW;
    }
}
