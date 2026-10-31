package org.pangrova.parser.ast.statement;

import java.util.Map;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

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
