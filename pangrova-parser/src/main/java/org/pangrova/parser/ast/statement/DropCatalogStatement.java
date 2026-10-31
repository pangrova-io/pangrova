package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents DROP CATALOG name statement.
 */
public record DropCatalogStatement(String name) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_CATALOG;
    }
}
