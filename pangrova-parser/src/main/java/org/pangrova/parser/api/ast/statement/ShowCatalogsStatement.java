package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents SHOW CATALOGS statement.
 */
public record ShowCatalogsStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_CATALOGS;
    }
}
