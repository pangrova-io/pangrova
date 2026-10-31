package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code SHOW MATERIALIZED VIEWS [LIKE ...]}.
 *
 * @param pattern optional SQL LIKE pattern matched against the full object name
 */
public record ShowMaterializedViewsStatement(String pattern) implements Statement {

    public ShowMaterializedViewsStatement() {
        this(null);
    }

    @Override
    public StatementType type() {
        return StatementType.SHOW_MATERIALIZED_VIEWS;
    }
}
