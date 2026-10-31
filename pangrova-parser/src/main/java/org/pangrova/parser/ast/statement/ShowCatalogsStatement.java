package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents SHOW CATALOGS statement.
 */
public record ShowCatalogsStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_CATALOGS;
    }
}
