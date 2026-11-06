package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents DESCRIBE CATALOG name statement.
 */
public record DescribeCatalogStatement(String name) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_CATALOG;
    }
}
