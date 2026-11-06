package org.pangrova.parser.api.ast.statement;

import java.util.Map;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents CREATE EMBEDDING MODEL name (key = value, ...).
 */
public record CreateEmbeddingModelStatement(
        String modelName,
        Map<String, String> properties) implements Statement {

    public CreateEmbeddingModelStatement {
        properties = Map.copyOf(properties);
    }

    @Override
    public StatementType type() {
        return StatementType.CREATE_EMBEDDING_MODEL;
    }
}
