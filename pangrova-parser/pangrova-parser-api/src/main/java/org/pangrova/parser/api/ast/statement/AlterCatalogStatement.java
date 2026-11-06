package org.pangrova.parser.api.ast.statement;

import java.util.Map;

import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents ALTER CATALOG name SET PROPERTIES (...) statement.
 */
public record AlterCatalogStatement(String name, Map<String, Literal> properties) implements Statement {

    public AlterCatalogStatement {
        properties = Map.copyOf(properties);
    }

    @Override
    public StatementType type() {
        return StatementType.ALTER_CATALOG;
    }
}
