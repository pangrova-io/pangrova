package org.pangrova.parser.ast.statement;

import java.util.Map;

import org.pangrova.parser.ast.Statement;

/**
 * Represents ALTER EMBEDDING MODEL name SET (key = value, ...).
 */
public record AlterEmbeddingModelStatement(
        String modelName,
        Map<String, String> properties) implements Statement {

    public AlterEmbeddingModelStatement {
        properties = Map.copyOf(properties);
    }
}
