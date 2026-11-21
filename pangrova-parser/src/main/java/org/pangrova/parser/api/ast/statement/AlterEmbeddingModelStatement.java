package org.pangrova.parser.api.ast.statement;

import java.util.Map;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents ALTER EMBEDDING MODEL name SET (key = value, ...).
 */
public record AlterEmbeddingModelStatement(
        String modelName,
        Map<String, String> properties) implements Statement {

    public AlterEmbeddingModelStatement {
        properties = Map.copyOf(properties);
    }

    @Override
    public StatementType type() {
        return StatementType.ALTER_EMBEDDING_MODEL;
    }
}
