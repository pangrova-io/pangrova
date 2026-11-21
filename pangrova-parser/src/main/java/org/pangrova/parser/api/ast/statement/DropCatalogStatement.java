package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents DROP CATALOG name statement.
 */
public record DropCatalogStatement(String name) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_CATALOG;
    }
}
