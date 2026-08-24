package org.pangrova.parser.ast.statement;

import java.util.Map;

import org.pangrova.parser.ast.Statement;

/**
 * Represents CREATE EMBEDDING MODEL name (key = value, ...).
 */
public record CreateEmbeddingModelStatement(
        String modelName,
        Map<String, String> properties) implements Statement {

    public CreateEmbeddingModelStatement {
        properties = Map.copyOf(properties);
    }
}
