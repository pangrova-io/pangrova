package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code SHOW MATERIALIZED VIEWS [LIKE ...]}.
 *
 * @param pattern optional SQL LIKE pattern matched against the full object name
 */
public record ShowMaterializedViewsStatement(String pattern) implements Statement {

    public ShowMaterializedViewsStatement() {
        this(null);
    }
}
