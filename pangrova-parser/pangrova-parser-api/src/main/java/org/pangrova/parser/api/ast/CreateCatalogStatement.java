package org.pangrova.parser.api.ast;

import java.util.Map;
import java.util.Optional;

/**
 * Represents CREATE CATALOG name [USING connectorType] WITH (...) statement.
 */
public record CreateCatalogStatement(
        String name,
        Optional<String> connectorType,
        Map<String, Literal> properties) implements Statement {

    public CreateCatalogStatement {
        properties = Map.copyOf(properties);
    }

    @Override
    public StatementType type() {
        return StatementType.CREATE_CATALOG;
    }
}
