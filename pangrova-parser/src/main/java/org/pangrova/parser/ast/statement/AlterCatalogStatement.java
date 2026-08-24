package org.pangrova.parser.ast.statement;

import java.util.Map;

import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.Statement;

/**
 * Represents ALTER CATALOG name SET PROPERTIES (...) statement.
 */
public record AlterCatalogStatement(String name, Map<String, Literal> properties) implements Statement {

    public AlterCatalogStatement {
        properties = Map.copyOf(properties);
    }
}
